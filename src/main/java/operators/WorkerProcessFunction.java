package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.internals.*;
import fgm.WorkerFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import state.WorkerStateHandler;

import static fgm.WorkerFunction.*;

public class WorkerProcessFunction<AccType, VectorType>  extends KeyedCoProcessFunction<String, InternalStream, InternalStream, InternalStream> {

    private final BaseConfig<AccType, VectorType, ?> cfg;

    public WorkerProcessFunction(BaseConfig<AccType, VectorType, ?> config){
        this.cfg = config;
    }
    private WorkerStateHandler<VectorType> state;

    @Override
    public void processElement1(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
        //System.out.println("id:"+context.getCurrentKey()+", type:"+input.getClass().getName());
        if(cfg.slidingWindowEnabled()) {
            state.setLastTs(context.timestamp());
            updateDrift(state, ((WindowSlide<AccType>) input).getVector(), cfg);
        }
        else {
            state.setLastTs(((Input) input).getTimestamp());
            updateDriftCashRegister(state, input, cfg);
        }
        subRoundProcess(state, collector, cfg);
    }

    @Override
    public void processElement2(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
        //System.out.println("id:"+context.getCurrentKey()+", type:"+input.getClass().getName());

        switch (input.type){
            case "GlobalEstimate":
                newRound(state, ((GlobalEstimate<VectorType>) input).getVector(), cfg);
                subRoundProcess(state, collector, cfg);
                break;
            case "Quantum":
                newSubRound(state, ((Quantum) input).getPayload());
                subRoundProcess(state, collector, cfg);
                break;
            case "RequestDrift":
                sendDrift(state, collector);
                break;
            case "RequestZeta":
                sendZeta(state, collector);
                break;
            case "Lambda":
                newRebalancedRound(state, ((Lambda) input).getLambda(), cfg);
                subRoundProcess(state, collector, cfg);
                break;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = new WorkerStateHandler<>(getRuntimeContext(), cfg);
    }
}
