package utils;

import org.apache.flink.api.java.utils.ParameterTool;

public class Misc {

    public static void printExecutionMessage(ParameterTool parameters) {
        String msg = "Executing Flink job with parameters: \n";
        msg += "\t--input-topic : "+parameters.get("input-topic", "kafka-input-topic")+"\n";
        msg += "\t--feedback-topic : "+parameters.get("feedback-topic", "kafka-feedback-topic")+"\n";

        System.out.println(msg);
    }
}
