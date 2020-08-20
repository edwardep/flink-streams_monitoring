package utils;

import org.apache.flink.api.java.utils.ParameterTool;

import static utils.DefJobParameters.*;

public class Misc {

    public static void printExecutionMessage(ParameterTool parameters) {
        String msg = "Executing Flink job with parameters: \n";
        msg += "\t--input-topic : "+parameters.get("input-topic", defInputTopic)+"\n";
        msg += "\t--feedback-topic : "+parameters.get("feedback-topic", defFeedbackTopic)+"\n";
        msg += "\t--output : "+parameters.get("output", defOutputPath)+"\n";
        msg += "\t--parallelism : "+parameters.getInt("parallelism", defParallelism)+"\n";
        msg += "\t--window : "+parameters.getInt("window", defWindowSize)+"\n";
        msg += "\t--slide : "+parameters.getInt("slide", defSlideSize)+"\n";
        msg += "\t--warmup : "+parameters.getInt("warmup", defWarmup)+"\n";
        msg += "\t--jobName : "+parameters.get("jobName", defJobName)+"\n";
        System.out.println(msg);
    }
}
