package fgm;

import configurations.BaseConfig;

import datatypes.InternalStream;
import datatypes.internals.Drift;
import datatypes.internals.Increment;
import datatypes.internals.Input;
import datatypes.internals.Zeta;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.WorkerStateHandler;
import java.io.IOException;
import java.io.Serializable;

import static utils.Metrics.WorkerMetrics.SENT_INCREMENT;
import static utils.Metrics.collectMetric;


/**
 * The WorkerFunction class contains all the required FGM Worker Node methods.
 */
public class WorkerFunction implements Serializable {
    private transient static Logger LOG = LoggerFactory.getLogger(WorkerFunction.class);

    /**
     * The second argument (input) might change from RecordType to VectorType when SlidingWindow gets implemented.
     */
    public static <VectorType> void updateDrift(WorkerStateHandler<VectorType> state,
                                                InternalStream input,
                                                BaseConfig<VectorType> cfg) throws Exception {
        //if (lambda = 1.0) it does nothing
        //((Input)input).setVal(((Input)input).getVal()/state.getLambda());
        state.setDrift(cfg.updateVector(input, state.getDrift()));
    }

    /**
     * At the beginning of a Round, the Coordinator initializes the safeFunction's hyperparameters<br>
     * and the Workers start the sub-round phase.<br>
     * The drift vector is being cleared and the first Quantum can be calculated locally<br>
     * with the following formula: Quantum = - fi(0)/2.<br>
     *
     */
    public static <VectorType> void newRound(WorkerStateHandler<VectorType> state,
                                             VectorType vector,
                                             BaseConfig<VectorType> cfg) throws Exception {

        state.setEstimate(vector);
        SafeZone sz = cfg.initializeSafeZone(vector);
        state.setSafeZone(sz);

        state.setLambda(1.0);

        double fi0 = cfg.safeFunction(cfg.newVectorInstance(), state.getEstimate(), sz);

        state.setLastZeta(fi0);

        // Compute the quantum value
        state.setQuantum(fi0/2);
        assert state.getQuantum() > 0d;

        // Start SubRound phase
        initializeSubround(state);
        state.setSubRoundPhase(true);
    }

    public static <VectorType> void newRebalancedRound(WorkerStateHandler<VectorType> state,
                                                       Double lambda,
                                                       BaseConfig<VectorType> cfg) throws Exception {
        // save lambda
        state.setLambda(lambda);

        // scale drift vector
        //state.setDrift(cfg.scaleVector(state.getDrift(), 1/lambda));

        // compute fi = lambda * fi(X/lambda)
        state.setFi(lambda * cfg.safeFunction(cfg.scaleVector(state.getDrift(), 1/lambda), state.getEstimate(), state.getSafeZone()));
    }
    /**
     * Emits a <i>POJO</i> containing the <b>drift vector</b> and other info.<br>
     * This function is called at the end of a Round.<br>
     */
    public static <VectorType> void sendDrift(WorkerStateHandler<VectorType> state,
                                              Collector<InternalStream> out) throws Exception {

        // Send Drift Vector to the Coordinator
        out.collect(new Drift<>(state.getCurrentSlideTimestamp(), state.getDrift()));

        // Clear the drift vector
        state.setDrift(null);
    }

    /**
     * Emits a <i>POJO</i> containing the <b>fi value</b> when requested. That happens when a violation occurs<br>
     * at the Coordinator: sum(Ci) &gt; k <br>
     */
    public static <VectorType> void sendZeta(WorkerStateHandler<VectorType> state,
                                             Collector<InternalStream> out) throws IOException {

        // halt sub-round phase
        state.setSubRoundPhase(false);

        state.setLastZeta(state.getFi());

        out.collect(new Zeta(state.getFi()));
    }

    /**
     * At the beginning of a sub-round, the coordinator ships the quantum value to all worker nodes. <br>
     * Each local node saves the quantum value and initializes the sub-round phase.<br>
     */
    public static <VectorType> void newSubRound(WorkerStateHandler<VectorType> state,
                                                Double payload) throws IOException {

        if (payload == null) {
            System.err.println("The received payload (Quantum) is empty. Sub-Round phase will not restart.");
            return;
        }

        state.setQuantum(payload);
        assert state.getQuantum() > 0d;

        // restart sub-round phase
        initializeSubround(state);
        state.setSubRoundPhase(true);
    }

    /**
     * A new SubRound begins, initialize Z and C_i
     */
    public static <VectorType> void initializeSubround(WorkerStateHandler<VectorType> state) throws IOException {
        state.setZeta(state.getLastZeta());
        state.setLocalCounter(0);
    }
    /**
     * The subRound process is called on every input record as long as the
     * {@link WorkerStateHandler#getSubRoundPhase() SubRoundPhase} is <b>enabled</b>.
     *
     *
     * It contains three distinct code blocks: <br>
     *     <ul>
     *         <li>Sub Round initialization (counter reset, compute Z(Xi))</li>
     *         <li>Computing Increment*</li>
     *         <li>Collecting Increment</li>
     *     </ul>
     * <br>
     * Computing Increment* : <code>increment = old_counter - max(old_counter, new_counter)</code>, <br>
     *     where <code>new_counter = (int) (( &phi;(Xi) - Z(Xi) ) / Quantum) </code> according to the fgm protocol.

     */
    public static <VectorType> void subRoundProcess(WorkerStateHandler<VectorType> state,
                                                    Collector<InternalStream> out,
                                                    BaseConfig<VectorType> cfg,
                                                    KeyedCoProcessFunction<?,?,?,?>.Context ctx) throws Exception {

        /*  While waiting for new Round or new SubRound, do nothing */
        if (!state.getSubRoundPhase())
            return;

        /*  If drift hasn't been updated since subRoundPhase stopped, do nothing */
        if (state.getDrift().equals(cfg.newVectorInstance()))
            return;

        /*  Compute new phi(X) value */
        state.setFi(state.getLambda() * cfg.safeFunction(cfg.scaleVector(state.getDrift(), 1/state.getLambda()), state.getEstimate(), state.getSafeZone()));

        /*  Compute new local Counter and get the increment */
        int old_counter = state.getLocalCounter();
        int new_counter = (int) ((state.getZeta() - state.getFi()) / state.getQuantum());
        int increment = new_counter - old_counter;

        /*  IF Ci has increased, send the increment and save the new_counter */
        if (increment > 0) {
            state.setLocalCounter(new_counter);
            out.collect(new Increment(increment));
            collectMetric(SENT_INCREMENT, state, ctx);
        }
    }

    /**
     * It delays the first round of FGM while allowing the local drift vectors to accumulate data. It achieves that
     * by registering an eventTime timer, which triggers the first drift flush after a constant amount of time.
     * @param state The Worker state handler
     * @param ctx   Context of the workers' processFunction
     * @param delay The delay value ( e.g Time.seconds(10))
     * @param <VectorType>  The datatype of the state vector
     * @throws IOException
     */
    public static <VectorType> void warmup(WorkerStateHandler<VectorType> state, KeyedCoProcessFunction.Context ctx, Time delay) throws IOException {
        if(state.getCurrentSlideTimestamp() == 0)
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + delay.toMilliseconds());
    }
}
