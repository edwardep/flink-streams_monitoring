package utils;

import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import state.CoordinatorStateHandler;
import state.WorkerStateHandler;

import java.io.IOException;

import static jobs.MonitoringJobWithKafka.*;

public class Metrics {

    public static <VectorType> void collectThroughput(WorkerStateHandler<VectorType> state,
                                                      KeyedCoProcessFunction<?,?,?,?>.Context ctx) throws IOException {

        String string = System.currentTimeMillis() + "," +
                state.getCurrentSlideTimestamp() + "," +
                ctx.getCurrentKey() + "," +
                state.getUpdates();
        ctx.output(localThroughput, string);
    }
    public static <VectorType> void collectMetric(WorkerMetrics type,
                                                  WorkerStateHandler<VectorType> state,
                                                  KeyedCoProcessFunction<?,?,?,?>.Context ctx) throws IOException {
        String string = System.currentTimeMillis() + "," +
                state.getCurrentSlideTimestamp() + "," +
                ctx.getCurrentKey() + "," +
                type;
        ctx.output(workerMetrics, string);
    }

    public static <VectorType> void collectMetric(CoordinatorMetrics type,
                                                  CoordinatorStateHandler<VectorType> state,
                                                  CoProcessFunction.Context ctx) throws IOException {
        ctx.output(coordinatorMetrics, System.currentTimeMillis()+","+state.getLastTs()+","+type);
    }


    public enum CoordinatorMetrics {
        ROUND,
        SUBROUND,
        REBALANCED_ROUND,
        SENT_ESTIMATE,
        SENT_QUANTUM,
        SENT_REQ_DRIFT,
        SENT_REQ_ZETA,
        SENT_LAMBDA,
        RECEIVED_INCREMENT,
        RECEIVED_DRIFT,
        RECEIVED_ZETA
    }
    public enum WorkerMetrics {
        EVENT,
        SENT_DRIFT,
        SENT_ZETA,
        SENT_INCREMENT,
        RECEIVED_ESTIMATE,
        RECEIVED_QUANTUM,
        RECEIVED_REQ_DRIFT,
        RECEIVED_REQ_ZETA,
        RECEIVED_LAMBDA
    }
}
