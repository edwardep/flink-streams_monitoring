package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import fgm.WorkerFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import state.WorkerStateHandler;

public class WorkerProcessFunction<VectorType>  extends KeyedCoProcessFunction<Integer, InternalStream, InternalStream, InternalStream> {

    private BaseConfig<VectorType, ?> cfg;

    public WorkerProcessFunction(BaseConfig<VectorType, ?> config){
        this.cfg = config;
    }

    private WorkerFunction<VectorType> fgm;
    private WorkerStateHandler<VectorType> state;

    @Override
    public void processElement1(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
        if(input.getRecord() == null)
            return;

        state.setLastTs(context.timestamp());

        fgm.updateDrift(state, (VectorType) input.getVector());
    }

    @Override
    public void processElement2(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
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
