package misc;

import configurations.AGMSConfig;
import configurations.TestP1Config;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.Input;
import datatypes.internals.WindowSlide;
import operators.IncAggregation;
import operators.WindowFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;
import sources.SyntheticEventTimeSource;

import sources.WorldCupMapSource;
import test_utils.Testable;

import java.io.Serializable;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;


public class SlidingWindow {

    @Test
    public void watermarkAssign_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(10);

        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/test_set.txt";
        AGMSConfig config = new AGMSConfig();

        env
                .readTextFile(defInputPath)
                .flatMap(new WorldCupMapSource(config))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                    @Override
                    public long extractAscendingTimestamp(InternalStream inputRecord) {
                        return ((Input)inputRecord).getTimestamp();
                    }
                })
                .keyBy(InternalStream::getStreamID)
                .timeWindow(Time.seconds(1000), Time.seconds(5))
                .process(new ProcessWindowFunction<InternalStream, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<InternalStream> iterable, Collector<String> collector) throws Exception {
                            collector.collect(iterable.toString());
                    }
                })
                .print();
//                .aggregate(
//                        new IncAggregation<>(config),
//                        new WindowFunction<>(config),
//                        config.getAccType(),                        // AggregateFunction ACC type
//                        config.getAccType(),                        // AggregateFunction V type
//                        TypeInformation.of(InternalStream.class))   // WindowFunction R type
//                .print();

        env.execute();
    }



    @Test
    public void watermarkAssign_kafka_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        String topic = "input";
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "test-group-1");
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
        AGMSConfig config = new AGMSConfig();

        env
                .addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), consumerProps).setStartFromEarliest())
                .flatMap(new WorldCupMapSource(config))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                    @Override
                    public long extractAscendingTimestamp(InternalStream inputRecord) {
                        return ((Input)inputRecord).getTimestamp();
                    }
                })
                .keyBy(InternalStream::getStreamID)
                .timeWindow(Time.seconds(1000), Time.seconds(5))
                .process(new ProcessWindowFunction<InternalStream, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<InternalStream> iterable, Collector<String> collector) throws Exception {
                        collector.collect(iterable.toString());
                    }
                })
                .print();
//                .aggregate(
//                        new IncAggregation<>(config),
//                        new WindowFunction<>(config),
//                        config.getAccType(),                        // AggregateFunction ACC type
//                        config.getAccType(),                        // AggregateFunction V type
//                        TypeInformation.of(InternalStream.class))   // WindowFunction R type
//                .print();

        env.execute();
    }



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
                .addSource(new SyntheticEventTimeSource(100000, 10, 100, 5, 10))
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(k->"0");

        DataStream<InternalStream> windowedStream1 = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(
                        new IncAggregation<>(cfg),
                        new WindowFunctionSimple<>(),
                        TypeInformation.of(Vector.class),
                        TypeInformation.of(Vector.class),
                        TypeInformation.of(InternalStream.class));

        DataStream<InternalStream> windowedStream2 = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(
                        new IncAggregation<>(cfg),
                        new WindowFunction<>(cfg),
                        TypeInformation.of(Vector.class),
                        TypeInformation.of(Vector.class),
                        TypeInformation.of(InternalStream.class));


        windowedStream1
                .process(new ProcessFunction<InternalStream, String>() {
                    @Override
                    public void processElement(InternalStream input, Context context, Collector<String> collector) throws Exception {
                        collector.collect(cfg.queryFunction(((WindowSlide<Vector>)input).getVector(), context.timestamp()));
                    }
                }).addSink(new Testable.WindowValidationSink1());

        windowedStream2
                .process(new ProcessFunction<InternalStream, String>() {

                    Vector drift = new Vector();
                    @Override
                    public void processElement(InternalStream input, Context context, Collector<String> collector) throws Exception {
                        Vector current = new Vector(((WindowSlide<Vector>)input).getVector().map());
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

    public static class WindowFunctionSimple<AccType> extends ProcessWindowFunction<AccType, InternalStream, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<AccType> iterable, Collector<InternalStream> out) {
            if(iterable.iterator().hasNext()){
                out.collect(new WindowSlide<>(key, iterable.iterator().next()));
            }
        }
    }

}
