package fgm;


import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.*;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sketches.AGMSSketch;
import sketches.SketchMath;
import state.CoordinatorStateHandler;

import java.io.IOException;

import static jobs.MonitoringJob.Q_estimate;


/**
 * The CoordinatorFunction class contains all the required FGM Coordinator Node methods.
 */
public class CoordinatorFunction {
    private transient static Logger LOG = LoggerFactory.getLogger(CoordinatorFunction.class);

    private final static long TIMEOUT = 86400000L;    // timeout before broadcasting SigInt (1 day in EventTime)

    /**
     *  Aggregates drift vectors. Also updates the hyper-parameters.
     */
    public static <VectorType> void handleDrift(CoordinatorStateHandler<VectorType> state,
                                                Drift<VectorType> input,
                                                CoProcessFunction.Context ctx,
                                                Collector<InternalStream> collector,
                                                BaseConfig<VectorType> cfg) throws Exception {

        if (state.getNodeCount() < cfg.workers()) {
            state.setNodeCount(state.getNodeCount() + 1);

            /* Aggregate drift vectors */
            state.setAggregateState(cfg.addVectors(input.getVector(), state.getAggregateState()));

            /* Save the last positive timestamp */
            if (input.getTimestamp() > 0) state.setLastTs(input.getTimestamp());
        }
        if(state.getNodeCount().equals(cfg.workers())){
            state.setNodeCount(0);

            // This allows rebalancing to happen only once per round and not in the first round
            if(cfg.rebalancingEnabled() && state.getSafeZone() != null && state.getLambda() == 1.0)
                rebalanceBimodal(state, collector, cfg);
            else
                newRound(state, ctx, collector, cfg);
        }
    }

    private static <VectorType> void newRound(CoordinatorStateHandler<VectorType> state,
                                              CoProcessFunction.Context ctx,
                                              Collector<InternalStream> collector,
                                              BaseConfig<VectorType> cfg) throws Exception {

        /* Update Global estimate */

        VectorType vec = cfg.addVectors(
                state.getEstimate(),
                cfg.scaleVector(state.getAggregateState(), 1.0/cfg.workers()));

        // Compute difference of Enew - Eold
//        double oldEst = SketchMath.median(((AGMSSketch) state.getEstimate()).values());
//        double newEst = SketchMath.median(((AGMSSketch)vec).values());
//        double diff = (Math.abs(oldEst - newEst) / newEst) * 100;
//        System.out.println("Round "+state.getRoundsCounter()+", diff = "+String.format("%.2f", diff)+"%");

        state.setEstimate(vec);
        state.setSafeZone(cfg.initializeSafeZone(vec));

        /* Begin subRounds phase*/
        broadcast_Estimate(state.getEstimate(), collector, cfg);

        /*  Wait asynchronously for increment values */
        state.setSync(false);

        /*  Monitored Query : Q(E) = Sum( E[i]^2 ) for each i in E */
        ctx.output(Q_estimate, cfg.queryFunction(state.getEstimate(), state.getLastTs()));

        /*  Cleanup */
        state.setAggregateState(null);
        state.setPsiBeta(0.0);
        state.setLambda(1.0);

        //System.out.println("rounds: "+ (++rounds));
        state.getRoundsCounter().add(1);
    }

    /**
     * This function aggregates the incoming &phi;(Xi) values to &psi;. When all nodes have transmitted, the &psi; value<br>
     * is compared to a threshold <b>T</b>. If &psi; exceeds <b>T</b> then a <b>new round</b> begins, otherwise, a <br>
     * new quantum is calculated and broadcasted back to the worker nodes.<br>
     * <br><br>
     * &psi; = &sum;&phi;(Xi)<br>
     * T = {@link BaseConfig#getMQF() quantizationFactor} * {@link BaseConfig#() parallelism} * {@link BaseConfig#safeFunction &phi;(0)}
     *
     */
    public static <VectorType> void handleZeta(CoordinatorStateHandler<VectorType> state,
                                               Double payload,
                                               Collector<InternalStream> collector,
                                               BaseConfig<VectorType> cfg) throws Exception {

        /*  Aggregate the incoming Phi(Xi) values to Psi */
        if (state.getNodeCount() < cfg.workers()) {
            state.setNodeCount(state.getNodeCount() + 1);
            state.setPsi(state.getPsi() + payload);
        }

        /*  Received all */
        if (state.getNodeCount().equals(cfg.workers())) {
            state.setNodeCount(0);

            // Compute threshold
            double T = cfg.getMQF()*cfg.workers()*cfg.safeFunction(cfg.newVectorInstance(), state.getEstimate(), state.getSafeZone());

            if(state.getPsi() + state.getPsiBeta() < T)
                broadcast_RequestDrift(collector, cfg);
            else
                startSubround(state, collector, cfg);
        }
    }

    /**
     * This function aggregates <b>increment values</b> to <b>globalCounter</b>. The fail condition is for the <b>globalCounter</b><br>
     * to exceed the system's parallelism <b>k</b>. When that happens, the Coordinator collects all &phi;(Xi) values.
     *
     * @param state The coordinator's {@link CoordinatorStateHandler state handler}
     * @param payload   Node i increment value
     * @param collector ProcessFunction collector
     * @throws IOException Flink exceptions
     */
    public static <VectorType> void handleIncrement(CoordinatorStateHandler<VectorType> state,
                                                    Integer payload,
                                                    Collector<InternalStream> collector,
                                                    BaseConfig<VectorType> cfg) throws IOException {

        /*  Do not proceed until a new SubRound begins */
        if(state.getSync())
            return;

        /*  Aggregate incoming increment values to the global counter */
        state.setGlobalCounter(state.getGlobalCounter() + payload);

        /*  If C > k : finish subRound */
        if(state.getGlobalCounter() > cfg.workers()) {

            broadcast_RequestZeta(collector, cfg);

            state.setPsi(0d);
            state.setGlobalCounter(0);

            /*  Enable sync and wait for Phi(Xi) values */
            state.setSync(true);

            //System.out.println("subRounds: "+ (++subRounds));
            state.getSubroundsCounter().add(1);
        }
    }

    public static <VectorType> void rebalanceBimodal(CoordinatorStateHandler<VectorType> state,
                                                     Collector<InternalStream> collector,
                                                     BaseConfig<VectorType> cfg) throws Exception {
        double lambda = 0.5;
        state.setLambda(lambda);
        updatePsiBeta(lambda, state, cfg);
        broadcast_Lambda(lambda, collector, cfg);
        state.getRebalancedRoundsCounter().add(1);
    }

    public static <VectorType> void startSubround(CoordinatorStateHandler<VectorType> state,
                                                  Collector<InternalStream> collector,
                                                  BaseConfig<VectorType> cfg) throws IOException {
        /*  Configuration is SAFE, broadcast new Quantum */
        Double quantum = (state.getPsi() + state.getPsiBeta()) / (2 * cfg.workers());

        broadcast_Quantum(quantum, collector, cfg);

        /*  Wait for increment values */
        state.setSync(false);
    }
    /**
     *  The broadcast_*() method is used by the coordinator every time it needs to communicate with the worker nodes.<br>
     *  It replicates the output object <b>k</b> times, where k = number of unique input streams.<br>
     *
     */
    private static <VectorType> void broadcast_Estimate(VectorType vector,
                                                        Collector<InternalStream> collector,
                                                        BaseConfig<VectorType> cfg) {
        for (int key = 0; key < cfg.workers(); key++)
            collector.collect(new GlobalEstimate<>(String.valueOf(key), vector));
    }
    public static void broadcast_RequestDrift(Collector<InternalStream> collector, BaseConfig<?> cfg) {
        for (int key = 0; key < cfg.workers(); key++)
            collector.collect(new RequestDrift(String.valueOf(key)));
    }
    private static void broadcast_RequestZeta(Collector<InternalStream> collector, BaseConfig<?> cfg) {
        for (int key = 0; key < cfg.workers(); key++)
            collector.collect(new RequestZeta(String.valueOf(key)));
    }
    private static void broadcast_Quantum(Double quantum, Collector<InternalStream> collector, BaseConfig<?> cfg) {
        for (int key = 0; key < cfg.workers(); key++)
            collector.collect(new Quantum(String.valueOf(key), quantum));
    }
    private static void broadcast_Lambda(Double lambda, Collector<InternalStream> collector, BaseConfig<?> cfg) {
        for (int key = 0; key < cfg.workers(); key++)
            collector.collect(new Lambda(String.valueOf(key), lambda));
    }
    public static void broadcast_SigInt(Collector<InternalStream> collector, BaseConfig<?> cfg) {
        for (int key = 0; key < cfg.workers(); key++)
            collector.collect(new SigInt(String.valueOf(key)));
    }

    // Psi_beta = (1 - lambda) * k * phi(BalanceVector / ((1 - lambda) * k))
    private static <VectorType> void updatePsiBeta(double lambda,
                                                   CoordinatorStateHandler<VectorType> state,
                                                   BaseConfig<VectorType> cfg) throws Exception {
        int k = cfg.workers();
        double mu = 1 - lambda;

        state.setPsiBeta(mu * k * cfg.safeFunction(cfg.scaleVector(
                state.getAggregateState(), 1/(mu * k)),
                state.getEstimate(),
                state.getSafeZone()));
    }

    public static <VectorType> void resetTimeoutTimer(long currentWatermark,
                                                      CoordinatorStateHandler<VectorType> state,
                                                      CoProcessFunction.Context ctx,
                                                      Collector<InternalStream> collector,
                                                      BaseConfig<VectorType> cfg) throws IOException {

        // In case a Final Watermark arrives
        if(currentWatermark >= Long.MAX_VALUE)
            broadcast_SigInt(collector, cfg);

        // Register timer for current watermark
        ctx.timerService().registerEventTimeTimer(currentWatermark + TIMEOUT);

        // Delete previous watermark if TIMEOUT period hasn't passed
        if(currentWatermark - state.getPrevWatermark() < TIMEOUT)
            ctx.timerService().deleteEventTimeTimer(state.getPrevWatermark() + TIMEOUT);

        // Store current watermark for next iteration
        state.setPrevWatermark(currentWatermark);
    }
}
