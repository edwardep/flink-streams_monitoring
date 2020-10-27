package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.internals.GlobalEstimate;
import datatypes.internals.Input;
import datatypes.internals.Lambda;
import datatypes.internals.Quantum;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import state.WorkerStateHandler;

import static fgm.WorkerFunction.*;
import static jobs.MonitoringJobWithKafka.localThroughput;

public class WorkerProcessFunction<VectorType>  extends KeyedCoProcessFunction<String, InternalStream, InternalStream, InternalStream> {

    private final BaseConfig<VectorType> cfg;

    public WorkerProcessFunction(BaseConfig<VectorType> config){
        this.cfg = config;
    }
    private WorkerStateHandler<VectorType> state;

    @Override
    public void processElement1(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
        //System.out.println("id:"+context.getCurrentKey()+", type:"+input.getClass().getName());
        long currentEventTimestamp = ((Input)input).getTimestamp();

        // Delay first drift flush
        warmup(state, context, cfg.warmup());

        // append input record to Drift
        updateDrift(state, input, cfg);

        // update records Counter
        state.setUpdates(state.getUpdates() + 1);

        // call subRoundProcess once every cfg.windowSlide() seconds
        if(currentEventTimestamp - state.getCurrentSlideTimestamp() >= cfg.windowSlide().toMilliseconds()) {
            state.setCurrentSlideTimestamp(currentEventTimestamp);
            subRoundProcess(state, collector, cfg);
            //System.out.println(context.getCurrentKey()+"> "+context.timerService().currentWatermark());

            context.output(localThroughput,context.getCurrentKey() + "," + System.currentTimeMillis() +","+ state.getUpdates());
        }
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
                sendZeta(state, collector);
                break;
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = new WorkerStateHandler<>(getRuntimeContext(), cfg);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InternalStream> out) throws Exception {
        sendDrift(state, out);
    }
}
