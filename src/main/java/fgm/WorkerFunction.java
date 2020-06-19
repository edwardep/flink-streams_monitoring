package fgm;

import configurations.BaseConfig;

import datatypes.InternalStream;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.WorkerStateHandler;
import java.io.IOException;
import java.io.Serializable;

import static datatypes.InternalStream.*;
import static java.lang.Math.max;


/**
 * The WorkerFunction class contains all the required FGM Worker Node methods.
 */
public class WorkerFunction<AccType, VectorType> implements Serializable {
    private transient static Logger LOG = LoggerFactory.getLogger(WorkerFunction.class);

    private BaseConfig<AccType, VectorType, ?> cfg;
    public WorkerFunction(BaseConfig<AccType, VectorType, ?> cfg) { this.cfg = cfg; }


    /**
     * The second argument (input) might change from RecordType to VectorType when SlidingWindow gets implemented.
     */
    public void updateDrift(WorkerStateHandler<VectorType> state, AccType input) throws Exception {
        state.setDrift(cfg.updateVector(input, state.getDrift()));
    }


    /**
     * At the beginning of a Round, the Coordinator initializes the safeFunction's hyperparameters<br>
     * and the Workers start the sub-round phase.<br>
     * The drift vector is being cleared and the first Quantum can be calculated locally<br>
     * with the following formula: Quantum = - fi(0)/2.<br>
     *
     */
    public void newRound(WorkerStateHandler<VectorType> state,
                         VectorType vector) throws Exception {

        state.setEstimate(vector);
        state.setSafeZone(cfg.initializeSafeZone(vector));

        state.setLambda(1.0);

        double fi0 = cfg.safeFunction(cfg.newInstance(), state.getEstimate(), state.getSafeZone());

        state.setLastZeta(fi0);

        // Compute the quantum value
        state.setQuantum(-fi0/2);
        assert state.getQuantum() > 0d;

        // Start SubRound phase
        state.setSubRoundPhase(true);
        state.setSubRoundInit(true);
    }

    /**
     * Emits a <i>POJO</i> containing the <b>drift vector</b> and other info.<br>
     * This function is called at the end of a Round.<br>
     */
    public void sendDrift(WorkerStateHandler<VectorType> state,
                          Collector<InternalStream> out) throws Exception {

        // Send Drift Vector to the Coordinator
        out.collect(downstreamDrift(state.getLastTs(), state.getDrift()));

        // Clear the drift vector
        state.setDrift(null);
    }

    /**
     * Emits a <i>POJO</i> containing the <b>fi value</b> when requested. That happens when a violation occurs<br>
     * at the Coordinator: sum(Ci) &gt; k <br>
     */
    public void sendZeta(WorkerStateHandler<VectorType> state,
                         Collector<InternalStream> out) throws IOException {

        // halt sub-round phase
        state.setSubRoundPhase(false);

        state.setLastZeta(state.getFi());

        out.collect(downstreamZeta(state.getFi()));
    }

    /**
     * At the beginning of a sub-round, the coordinator ships the quantum value to all worker nodes. <br>
     * Each local node saves the quantum value and initializes the sub-round phase.<br>
     */
    public void newSubRound(WorkerStateHandler<VectorType> state,
                            Double payload) throws IOException {

        if (payload == null) {
            System.err.println("The received payload (Quantum) is empty. Sub-Round phase will not restart.");
            return;
        }

        state.setQuantum(payload);
        assert state.getQuantum() > 0d;

        // restart sub-round phase
        state.setSubRoundPhase(true);
        state.setSubRoundInit(true);
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
    public void subRoundProcess(WorkerStateHandler<VectorType> state,
                                Collector<InternalStream> out) throws Exception {

        /*  While waiting for new Round or new SubRound, do nothing */
        if (!state.getSubRoundPhase())
            return;

        /*  If drift hasn't been updated since subRoundPhase stopped, do nothing */
        if (state.getDrift().equals(cfg.newInstance()))
            return;

        /*  new SubRound begins, initialize Z and Ci */
        if (state.getSubRoundInit()) {
            state.setZeta(state.getLastZeta());
            state.setLocalCounter(0);
            state.setSubRoundInit(false);
        }

        /*  Compute new phi(X) value */
        state.setFi(
                state.getLambda() * cfg.safeFunction(
                        cfg.scaleVector(state.getDrift(),1/state.getLambda()),
                        state.getEstimate(),
                        state.getSafeZone()));

        /*  Compute new local Counter and get the increment */
        int old_counter = state.getLocalCounter();
        int new_counter = (int) ((state.getFi() - state.getZeta()) / state.getQuantum());
        int increment = new_counter - old_counter;

        /*  IF Ci has increased, send the increment and save the new_counter */
        if (increment > 0) {
            state.setLocalCounter(new_counter);
            out.collect(downstreamIncrement((double) increment));
        }
    }

    public void newRebalancedRound(WorkerStateHandler<VectorType> state,
                                   Double payload) throws Exception {

        // save lambda
        state.setLambda(payload);

        // Compute the quantum value
        double fi0 = state.getLambda() * cfg.safeFunction(cfg.newInstance(), state.getEstimate(), state.getSafeZone());
        state.setLastZeta(fi0);
        state.setQuantum(-fi0/2);

        // begin subRound phase
        state.setSubRoundPhase(true);
        state.setSubRoundInit(true);
    }
}
