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
import utils.Metrics;

import static fgm.WorkerFunction.*;
import static jobs.MonitoringJobWithKafka.localThroughput;
import static utils.Metrics.WorkerMetrics.*;
import static utils.Metrics.collectMetric;
import static utils.Metrics.collectThroughput;

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

        // Delay first drift flush (Disabled and moved to coordinator)
        //warmup(state, context, cfg.warmup());

        // append input record to Drift
        updateDrift(state, input, cfg);

        // update records Counter
        state.setUpdates(state.getUpdates() + 1);

        // call subRoundProcess once every cfg.windowSlide() seconds
        if(currentEventTimestamp - state.getCurrentSlideTimestamp() >= cfg.windowSlide().toMilliseconds()) {
            state.setCurrentSlideTimestamp(currentEventTimestamp);
            subRoundProcess(state, collector, cfg, context);

            // Output local throughput metric
            collectThroughput(state, context);
        }
    }

    @Override
    public void processElement2(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
        //System.out.println("id:"+context.getCurrentKey()+", type:"+input.getClass().getName());

        switch (input.type){
            case "GlobalEstimate":
                newRound(state, ((GlobalEstimate<VectorType>) input).getVector(), cfg);
                collectMetric(RECEIVED_ESTIMATE, state, context);
                subRoundProcess(state, collector, cfg, context);
                break;
            case "Quantum":
                newSubRound(state, ((Quantum) input).getPayload());
                collectMetric(RECEIVED_QUANTUM, state, context);
                subRoundProcess(state, collector, cfg, context);
                break;
            case "RequestDrift":
                sendDrift(state, collector);
                collectMetric(RECEIVED_REQ_DRIFT, state, context);
                collectMetric(SENT_DRIFT, state, context);
                break;
            case "RequestZeta":
                sendZeta(state, collector);
                collectMetric(RECEIVED_REQ_ZETA, state, context);
                collectMetric(SENT_ZETA, state, context);
                break;
            case "Lambda":
                newRebalancedRound(state, ((Lambda) input).getLambda(), cfg);
                collectMetric(RECEIVED_LAMBDA, state, context);
                sendZeta(state, collector);
                collectMetric(SENT_ZETA, state, context);
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
