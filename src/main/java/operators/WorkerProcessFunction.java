package operators;

import configurations.BaseConfig;
import datatypes.Accumulator;
import datatypes.InternalStream;
import datatypes.internals.*;
import fgm.WorkerFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import state.WorkerStateHandler;

public class WorkerProcessFunction<AccType, VectorType>  extends KeyedCoProcessFunction<String, Accumulator<AccType>, InternalStream, InternalStream> {

    private BaseConfig<AccType, VectorType, ?> cfg;

    public WorkerProcessFunction(BaseConfig<AccType, VectorType, ?> config){
        this.cfg = config;
    }

    private WorkerFunction<AccType, VectorType> fgm;
    private WorkerStateHandler<VectorType> state;

    @Override
    public void processElement1(Accumulator<AccType> input, Context context, Collector<InternalStream> collector) throws Exception {

        state.setLastTs(context.timestamp());
        assert state.getLastTs() > 0;

        fgm.updateDrift(state, input.getVec());
        fgm.subRoundProcess(state, collector);
    }

    @Override
    public void processElement2(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
       // System.out.println("id:"+context.getCurrentKey()+", type:"+input.getClass().getName());

        if (GlobalEstimate.class.equals(input.getClass())) {
            fgm.newRound(state, ((GlobalEstimate<VectorType>) input).getVector());
            fgm.subRoundProcess(state, collector);
        } else if (Quantum.class.equals(input.getClass())) {
            fgm.newSubRound(state, ((Quantum) input).getPayload());
            fgm.subRoundProcess(state, collector);
        } else if (RequestDrift.class.equals(input.getClass())) {
            fgm.sendDrift(state, collector);
        } else if (RequestZeta.class.equals(input.getClass())) {
            fgm.sendZeta(state, collector);
        } else if (Lambda.class.equals(input.getClass())) {
            fgm.newRebalancedRound(state, ((Lambda) input).getLambda());
            fgm.subRoundProcess(state, collector);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fgm = new WorkerFunction<>(cfg);
        state = new WorkerStateHandler<>(getRuntimeContext(), cfg);
    }
}
