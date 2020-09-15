package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.internals.*;
import fgm.WorkerFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import state.WorkerStateHandler;

public class WorkerProcessFunction<AccType, VectorType>  extends KeyedCoProcessFunction<String, InternalStream, InternalStream, InternalStream> {

    private final BaseConfig<AccType, VectorType, ?> cfg;

    public WorkerProcessFunction(BaseConfig<AccType, VectorType, ?> config){
        this.cfg = config;
    }

    private WorkerFunction<AccType, VectorType> fgm;
    private WorkerStateHandler<VectorType> state;

    @Override
    public void processElement1(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
        //System.out.println("id:"+context.getCurrentKey()+", type:"+input.getClass().getName());
        if(cfg.slidingWindowEnabled()) {
            state.setLastTs(context.timestamp());
            fgm.updateDrift(state, ((WindowSlide<AccType>) input).getVector());
        }
        else {
            state.setLastTs(((Input) input).getTimestamp());
            fgm.updateDriftCashRegister(state, input);
        }
        fgm.subRoundProcess(state, collector);
    }

    @Override
    public void processElement2(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
        System.out.println("id:"+context.getCurrentKey()+", type:"+input.getClass().getName());

        switch (input.type){
            case "GlobalEstimate":
                fgm.newRound(state, ((GlobalEstimate<VectorType>) input).getVector());
                fgm.subRoundProcess(state, collector);
                break;
            case "Quantum":
                fgm.newSubRound(state, ((Quantum) input).getPayload());
                fgm.subRoundProcess(state, collector);
                break;
            case "RequestDrift":
                fgm.sendDrift(state, collector);
                break;
            case "RequestZeta":
                fgm.sendZeta(state, collector);
                break;
            case "Lambda":
                fgm.newRebalancedRound(state, ((Lambda) input).getLambda());
                fgm.subRoundProcess(state, collector);
                break;
            case "SigInt":
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
