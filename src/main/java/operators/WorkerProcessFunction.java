package operators;

import configurations.BaseConfig;
import datatypes.Accumulator;
import datatypes.InternalStream;
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
        //System.out.println(input.toString());
        switch (input.getType()) {
            case HYPERPARAMETERS:
                fgm.newRound(state, (VectorType) input.getVector());
                fgm.subRoundProcess(state, collector);
                break;

            case QUANTUM:
                fgm.newSubRound(state, input.getPayload());
                fgm.subRoundProcess(state, collector);
                break;

            case REQ_DRIFT:
                fgm.sendDrift(state, collector);
                break;

            case REQ_ZETA:
                fgm.sendZeta(state, collector);
                break;

            case BALANCE:
                fgm.newRebalancedRound(state, input.getPayload());
                fgm.subRoundProcess(state, collector);
                break;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fgm = new WorkerFunction<>(cfg);
        state = new WorkerStateHandler<>(getRuntimeContext(), cfg);
    }
}
