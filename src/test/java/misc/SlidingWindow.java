package misc;

import configurations.FgmConfig;
import configurations.TestP1Config;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import operators.IncAggregation;
import operators.IncAggregation_def;
import operators.WindowFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.SyntheticEventTimeSource;

import test_utils.Testable;

import static junit.framework.TestCase.*;


public class SlidingWindow {

    @Test
    public void slidingWindow_vector_diff_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int slide = 5;
        int window = 1000;

        TestP1Config cfg = new TestP1Config();

        KeyedStream<InputRecord, String> keyedStream = env
                //.addSource(new SyntheticEventTimeSource())
                .addSource(new SyntheticEventTimeSource())
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(k->"0");

        DataStream<InternalStream> windowedStream = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new IncAggregation_def(cfg), new WindowFunction<>());

        windowedStream
                .process(new ProcessFunction<InternalStream, String>() {
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        collector.collect(cfg.queryFunction((Vector) internalStream.getVector(), context.timestamp()));
                    }
                }).addSink(new Testable.WindowValidationSink1());

        windowedStream
                .process(new ProcessFunction<InternalStream, String>() {
                    Vector previous = new Vector();
                    Vector drift = new Vector();
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        long start1 = System.currentTimeMillis();
                        Vector current = new Vector(((Vector)internalStream.getVector()).map());
                        drift = cfg.addVectors(cfg.subtractVectors(current, previous), drift);
                        collector.collect(cfg.queryFunction(drift,context.timestamp()));
                        previous.map().putAll(current.map());
                        long time = System.currentTimeMillis() - start1;
                        if(time > 5) System.out.println("time: "+time);
                    }
                }).addSink(new Testable.WindowValidationSink2());

        env.execute();

        int size1 = Testable.WindowValidationSink1.result1.size();
        int size2 = Testable.WindowValidationSink2.result2.size();
        assert size1 == size2;
        for(int i = 0; i < size1; i++)
            assertEquals("at"+i+" out of "+size1, Testable.WindowValidationSink1.result1.get(i),Testable.WindowValidationSink2.result2.get(i));
    }

}
