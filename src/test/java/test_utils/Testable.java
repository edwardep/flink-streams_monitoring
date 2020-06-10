package test_utils;

import datatypes.InternalStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;


public class Testable {

    public static class intermediateSink implements SinkFunction<String> {

        public static String result = "";

        @Override
        public synchronized void invoke(String input, Context ctx) {
            result += input;
        }
    }

    public static class InternalStreamSink implements SinkFunction<InternalStream> {

        public static List<InternalStream> result = new ArrayList<>();

        @Override
        public synchronized void invoke(InternalStream input, Context ctx) {
            result.add(input);
        }


    }

    public static class WindowValidationSink1 implements SinkFunction<String> {

        public static List<String> result1 = new ArrayList<>();

        @Override
        public synchronized void invoke(String input, Context ctx) {
            result1.add(input);
        }
    }

    public static class WindowValidationSink2 implements SinkFunction<String> {

        public static List<String> result2 = new ArrayList<>();

        @Override
        public synchronized void invoke(String input, Context ctx) {
            result2.add(input);
        }
    }
}
