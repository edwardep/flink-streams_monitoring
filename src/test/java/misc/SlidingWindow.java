package misc;

import configurations.TestP1Config;
import datatypes.Accumulator;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import operators.IncAggregation;
import operators.WindowFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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


public class SlidingWindow {

    @Test
    public void slidingWindow_vector_diff_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
        int slide = 5;
        int window = 1000;

        TestP1Config cfg = new TestP1Config();

        KeyedStream<InputRecord, String> keyedStream = env
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

        DataStream<Accumulator<Vector>> windowedStream = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new IncAggregation<>(cfg), new WindowFunction<>(cfg));

        windowedStream
                .process(new ProcessFunction<Accumulator<Vector>, String>() {
                    @Override
                    public void processElement(Accumulator<Vector> internalStream, Context context, Collector<String> collector) throws Exception {
                        collector.collect(cfg.queryFunction(internalStream.getVec(), context.timestamp()));
                    }
                }).addSink(new Testable.WindowValidationSink1());

        env.execute();

        for(String str : Testable.WindowValidationSink1.result1)
            System.out.println(str);
    }

}
