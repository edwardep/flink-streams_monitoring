package utils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.utils.ParameterTool;

import static utils.DefJobParameters.*;

public class Misc {

    public static void printExecutionMessage(ParameterTool parameters) {
        String msg = "Executing Flink job with parameters: \n";
        msg += "\t--input-topic : "+parameters.get("input-topic", defInputTopic)+"\n";
        msg += "\t--feedback-topic : "+parameters.get("feedback-topic", defFeedbackTopic)+"\n";
        msg += "\t--output : "+parameters.get("output", defOutputPath)+"\n";
        msg += "\t--kafka-servers: "+parameters.get("kafka-servers", defKafkaServers)+"\n";
        msg += "\t--parallelism : "+parameters.getInt("parallelism", defParallelism)+"\n";
        msg += "\t--sliding-window: "+parameters.getBoolean("sliding-window")+"\n";
        msg += "\t--rebalancing: "+parameters.getBoolean("rebalance")+"\n";
        msg += "\t--window : "+parameters.getInt("window", defWindowSize)+"\n";
        msg += "\t--slide : "+parameters.getInt("slide", defSlideSize)+"\n";
        msg += "\t--warmup : "+parameters.getInt("warmup", defWarmup)+"\n";
        msg += "\t--jobName : "+parameters.get("jobName", defJobName)+"\n";
        msg += "\t--workers : "+parameters.getInt("workers", defWorkers)+"\n";
        msg += "\t--epsilon : "+parameters.getDouble("epsilon", defEpsilon)+"\n";
        System.out.println(msg);
    }

    @Deprecated
    public static void printExecutionResults(ParameterTool parameters, JobExecutionResult result) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(parameters.get("jobName", defJobName)).append(",");
        stringBuilder.append(parameters.getInt("parallelism", defParallelism)).append(",");
        stringBuilder.append(parameters.getInt("workers", defWorkers)).append(",");
        stringBuilder.append(parameters.getDouble("epsilon", defEpsilon)).append(",");
        stringBuilder.append(parameters.getInt("window", defWindowSize)).append(",");
        stringBuilder.append(parameters.getInt("slide", defSlideSize)).append(",");
        stringBuilder.append(parameters.getBoolean("sliding-window")).append(",");
        stringBuilder.append(parameters.getBoolean("rebalance")).append(",");
        stringBuilder.append(parameters.get("output", defOutputPath)).append(",");

        stringBuilder.append(result.getNetRuntime()).append(",");
        stringBuilder.append(result.getAccumulatorResult("roundsCounter").toString()).append(",");
        stringBuilder.append(result.getAccumulatorResult("subroundsCounter").toString()).append(",");
        stringBuilder.append(result.getAccumulatorResult("rebalancedRoundsCounter").toString()).append(",");


        System.out.println("======================================================================");
        System.out.println(stringBuilder);
        System.out.println("======================================================================");
    }
}
