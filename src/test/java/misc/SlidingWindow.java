package misc;

import configurations.BaseConfig;
import configurations.TestP1Config;
import datatypes.Accumulator;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import operators.IncAggregation;
import operators.WindowFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.SyntheticEventTimeSource;

import test_utils.Testable;

import java.io.IOException;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;


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

        DataStream<Accumulator<Vector>> windowedStream1 = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new IncAggregation<>(cfg), new WindowFunctionSimple<>());

        DataStream<Accumulator<Vector>> windowedStream2 = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new IncAggregation<>(cfg), new WindowFunction<>(cfg));


        windowedStream1
                .process(new ProcessFunction<Accumulator<Vector>, String>() {
                    @Override
                    public void processElement(Accumulator<Vector> input, Context context, Collector<String> collector) throws Exception {
                        collector.collect(cfg.queryFunction(input.getVec(), context.timestamp()));
                    }
                }).addSink(new Testable.WindowValidationSink1());

        windowedStream2
                .process(new ProcessFunction<Accumulator<Vector>, String>() {

                    Vector drift = new Vector();
                    @Override
                    public void processElement(Accumulator<Vector> input, Context context, Collector<String> collector) throws Exception {
                        Vector current = new Vector(input.getVec().map());
                        drift = cfg.addVectors(current, drift);
                        collector.collect(cfg.queryFunction(drift,context.timestamp()));
                    }
                }).addSink(new Testable.WindowValidationSink2());

        env.execute();

        int size1 = Testable.WindowValidationSink1.result1.size();
        int size2 = Testable.WindowValidationSink2.result2.size();
        assert size1 == size2;
        for(int i = 0; i < size1; i++)
            assertEquals("at"+i+" out of "+size1, Testable.WindowValidationSink1.result1.get(i),Testable.WindowValidationSink2.result2.get(i));
    }

    public static class WindowFunctionSimple<AccType> extends ProcessWindowFunction<Accumulator<AccType>, Accumulator<AccType>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Accumulator<AccType>> iterable, Collector<Accumulator<AccType>> out) {
            if(iterable.iterator().hasNext()){
                out.collect(new Accumulator<>(key, iterable.iterator().next().getVec()));
            }
        }
    }

}
