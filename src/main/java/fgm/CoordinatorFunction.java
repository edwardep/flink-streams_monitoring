package fgm;


import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.internals.*;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.CoordinatorStateHandler;

import java.io.IOException;

import static jobs.MonitoringJob.Q_estimate;


/**
 * The CoordinatorFunction class contains all the required FGM Coordinator Node methods.
 */
public class CoordinatorFunction<VectorType> {
    private transient static Logger LOG = LoggerFactory.getLogger(CoordinatorFunction.class);

    private BaseConfig<?, VectorType, ?> cfg;
    private long rounds = 0;
    private int subRounds = 0;

    private boolean rebalancing = true;
    public void disableRebalancing() { rebalancing = false; }

    private Double lambda = 1.0;

    private long lastTs = Long.MIN_VALUE;

    public CoordinatorFunction(BaseConfig<?, VectorType, ?> cfg){
        this.cfg = cfg;
    }

    /**
     *  Aggregates drift vectors. Also updates the hyper-parameters.
     */
    public void handleDrift(CoordinatorStateHandler<VectorType> state,
                            Drift<VectorType> input,
                            CoProcessFunction.Context ctx,
                            Collector<InternalStream> collector) throws Exception {

        if(state.getNodeCount() < cfg.uniqueStreams())
        {
            state.setNodeCount(state.getNodeCount() + 1);

            /* Aggregate drift vectors */
            state.setAggregateState(cfg.addVectors(input.getVector(), state.getAggregateState()));

            /* Save the last positive timestamp */
            if(input.getTimestamp() > 0) lastTs = input.getTimestamp();
        }

        /*  Received all */
        if(state.getNodeCount().equals(cfg.uniqueStreams()))
        {
            state.setNodeCount(0);

            // Handling first round
            if(lambda == 1.0) {
                newRound(state, ctx, collector);
                return;
            }

            // Psi_beta = (1 - lambda) * k * phi(BalanceVector / ((1 - lambda) * k))
            updatePsiBeta(state);

            broadcast_Lambda(lambda, collector);

            /*  Wait asynchronously for increment values */
            state.setSync(false);

            System.out.println("Rebalancing Round with lambda = 0.5");
            // reset
            lambda = 1.0;
        }
    }

    private void newRound(CoordinatorStateHandler<VectorType> state,
                          CoProcessFunction.Context ctx,
                          Collector<InternalStream> collector) throws Exception {

        /* Update Global estimate */

        VectorType vec = cfg.addVectors(
                state.getEstimate(),
                cfg.scaleVector(state.getAggregateState(), 1.0/cfg.uniqueStreams()));

        state.setEstimate(vec);
        state.setSafeZone(cfg.initializeSafeZone(vec));

        /* Begin subRounds phase*/
        broadcast_Estimate(state.getEstimate(), collector);

        /*  Wait asynchronously for increment values */
        state.setSync(false);

        /*  Monitored Query : Q(E) = Sum( E[i]^2 ) for each i in E */
        ctx.output(Q_estimate, cfg.queryFunction(state.getEstimate(), this.lastTs));

        /*  set lambda for upcoming rebalancing round */
        if(rebalancing) lambda = 0.5;

        /*  Cleanup */
        state.setAggregateState(null);
        state.setPsiBeta(0.0);

        System.out.println("rounds: "+ (++rounds));
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
    public void handleZeta(CoordinatorStateHandler<VectorType> state,
                           CoProcessFunction.Context ctx,
                           Double payload,
                           Collector<InternalStream> collector) throws Exception {

        /*  Aggregate the incoming Phi(Xi) values to Psi */
        if(state.getNodeCount() < cfg.uniqueStreams())
        {
            state.setNodeCount(state.getNodeCount() + 1);
            state.setPsi(state.getPsi() + payload);
        }

        /*  Received all */
        if(state.getNodeCount().equals(cfg.uniqueStreams()))
        {
            state.setNodeCount(0);

            double safeThreshold = cfg.getMQF()*cfg.uniqueStreams()*cfg.safeFunction(
                    cfg.newInstance(),
                    state.getEstimate(),
                    state.getSafeZone());

            if(state.getPsi() + state.getPsiBeta() > safeThreshold)
            {
                /*  Configuration is SAFE, broadcast new Quantum */
                Double quantum = (state.getPsi() + state.getPsiBeta()) / (2 * cfg.uniqueStreams());

                broadcast_Quantum(quantum, collector);

                /*  Wait for increment values */
                state.setSync(false);

            }
            else
            {
                if(rebalancing && lambda < 1.0)
                    broadcast_RequestDrift(collector);

                if(rebalancing && lambda == 1.0)
                    newRound(state, ctx, collector);

                if(!rebalancing)
                    broadcast_RequestDrift(collector);
            }
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
    public void handleIncrement(CoordinatorStateHandler<VectorType> state,
                                Integer payload,
                                Collector<InternalStream> collector) throws IOException {

        /*  Do not proceed until a new SubRound begins */
        if(state.getSync())
            return;

        /*  Aggregate incoming increment values to the global counter */
        state.setGlobalCounter(state.getGlobalCounter() + payload);

        /*  If C > k : finish subRound */
        if(state.getGlobalCounter() > cfg.uniqueStreams()) {

            broadcast_RequestZeta(collector);

            state.setPsi(0d);
            state.setGlobalCounter(0);

            /*  Enable sync and wait for Phi(Xi) values */
            state.setSync(true);

            System.out.println("subRounds: "+ (++subRounds));
        }
    }

    /**
     *  The broadcast_*() method is used by the coordinator every time it needs to communicate with the worker nodes.<br>
     *  It replicates the output object <b>k</b> times, where k = number of unique input streams.<br>
     *
     */
    private void broadcast_Estimate(VectorType vector, Collector<InternalStream> collector) {
        for (int key = 0; key < cfg.uniqueStreams(); key++)
            collector.collect(new GlobalEstimate<>(String.valueOf(key), vector));
    }
    public void broadcast_RequestDrift(Collector<InternalStream> collector) {
        for (int key = 0; key < cfg.uniqueStreams(); key++)
            collector.collect(new RequestDrift(String.valueOf(key)));
    }
    private void broadcast_RequestZeta(Collector<InternalStream> collector) {
        for (int key = 0; key < cfg.uniqueStreams(); key++)
            collector.collect(new RequestZeta(String.valueOf(key)));
    }
    private void broadcast_Quantum(Double quantum, Collector<InternalStream> collector) {
        for (int key = 0; key < cfg.uniqueStreams(); key++)
            collector.collect(new Quantum(String.valueOf(key), quantum));
    }
    private void broadcast_Lambda(Double lambda, Collector<InternalStream> collector) {
        for (int key = 0; key < cfg.uniqueStreams(); key++)
            collector.collect(new Lambda(String.valueOf(key), lambda));
    }

    // Psi_beta = (1 - lambda) * k * phi(BalanceVector / ((1 - lambda) * k))
    private void updatePsiBeta(CoordinatorStateHandler<VectorType> state) throws Exception {
        state.setPsiBeta(
                (1-lambda)*cfg.uniqueStreams()*cfg.safeFunction(
                        cfg.scaleVector(state.getAggregateState(), 1/((1-lambda)*cfg.uniqueStreams())),
                        state.getEstimate(), state.getSafeZone()));
    }
}
