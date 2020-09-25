package misc;

import configurations.AGMSConfig;
import configurations.TestP1Config;
import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.Input;
import datatypes.internals.WindowSlide;
import fgm.WorkerFunction;
import operators.IncAggregation;
import operators.WindowFunction;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sketches.AGMSSketch;
import sources.SyntheticEventTimeSource;

import sources.WorldCupMapSource;
import state.WorkerStateHandler;
import test_utils.Testable;
import operators.CustomSlidingWindow;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static kafka.KafkaUtils.createConsumerInput;


public class SlidingWindow {

    @Test
    public void customSlidingWindow_validation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1);
        ParameterTool parameters = ParameterTool.fromPropertiesFile("/home/edwardep/flink-streams_monitoring/src/main/java/properties/pico.properties");
        AGMSConfig config = new AGMSConfig(parameters);

        KeyedStream<InternalStream, String> keyedStream = env
                .addSource(createConsumerInput(parameters).setStartFromEarliest())
                .setParallelism(1)
                .flatMap(new WorldCupMapSource(config))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                    @Override
                    public long extractAscendingTimestamp(InternalStream inputRecord) {
                        return ((Input)inputRecord).getTimestamp();
                    }
                })
                .keyBy(InternalStream::getStreamID);


//        keyedStream
//                .timeWindow(Time.seconds(1000), Time.seconds(5))
//                .aggregate(
//                        new IncAggregation<>(config),
//                        new WindowFunction<>(config),
//                        config.getAccType(),
//                        config.getAccType(),
//                        TypeInformation.of(InternalStream.class))
//                .keyBy(InternalStream::getStreamID)
//                .process(new KeyedProcessFunction<String, InternalStream, String>() {
//                    WorkerStateHandler<AGMSSketch> state;
//                    @Override
//                    public void processElement(InternalStream input, Context context, Collector<String> collector) throws Exception {
//                        WorkerFunction.updateDrift(state, ((WindowSlide<Map<Tuple2<Integer, Integer>,Double>>)input).getVector(), config);
//                        collector.collect(config.queryFunction(state.getDrift(), context.timestamp()));
//                    }
//                    @Override
//                    public void open(Configuration parameters) {
//                        state = new WorkerStateHandler<>(getRuntimeContext(), config);
//                    }
//                })
//                .writeAsText("/home/edwardep/flink-streams_monitoring/logs/window1.txt", FileSystem.WriteMode.OVERWRITE);

        keyedStream
                .process(new CustomSlidingWindow(Time.seconds(1000), Time.seconds(5)))
                .keyBy(InternalStream::getStreamID)
                .process(new KeyedProcessFunction<String, InternalStream, String>() {
                    WorkerStateHandler<AGMSSketch> state;
                    long currentSlideTimestamp = 0L;
                    @Override
                    public void processElement(InternalStream input, Context context, Collector<String> collector) throws Exception {
                        long currentEventTimestamp = ((Input)input).getTimestamp();

                        if(currentEventTimestamp - currentSlideTimestamp >= 5000) {
                            currentSlideTimestamp = currentEventTimestamp;
                            collector.collect(config.queryFunction(state.getDrift(), ((Input) input).getTimestamp()));
                        }
                        WorkerFunction.updateDrift(state, input, config);
                    }
                    @Override
                    public void open(Configuration parameters) {
                        state = new WorkerStateHandler<>(getRuntimeContext(), config);
                    }
                })
                .writeAsText("/home/edwardep/flink-streams_monitoring/logs/window3.txt", FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult result = env.execute();
        System.out.println("exec time: "+result.getNetRuntime());
    }

    @Test
    public void customSlidingWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1);
        ParameterTool parameters = ParameterTool.fromPropertiesFile("/home/edwardep/flink-streams_monitoring/src/main/java/properties/pico.properties");
        AGMSConfig config = new AGMSConfig(parameters);

        env
                .addSource(createConsumerInput(parameters).setStartFromEarliest())
                .setParallelism(1)
                .flatMap(new WorldCupMapSource(config))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                    @Override
                    public long extractAscendingTimestamp(InternalStream inputRecord) {
                        return ((Input)inputRecord).getTimestamp();
                    }
                })
                .keyBy(InternalStream::getStreamID)
                .process(new CustomWindowFunction(Time.seconds(10), Time.seconds(2)))
                .print();

        env.execute();
    }
    public static class CustomWindowFunction extends KeyedProcessFunction<String, InternalStream, InternalStream> {

        long currentSlideTimestamp = 0L;
        long windowSize;
        long windowSlide;

        public CustomWindowFunction(Time size, Time slide) {
            this.windowSize = size.toMilliseconds();
            this.windowSlide = slide.toMilliseconds();
        }

        transient MapState<Long, List<InternalStream>> state;

        @Override
        public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
            long currentEventTimestamp = ((Input)internalStream).getTimestamp();

            // Assign new SlideTimestamp if currentEventTimestamp has exceeded this Slide's time span
            // and collect all corresponding events
            if(currentEventTimestamp - currentSlideTimestamp >= windowSlide) {
                collectAppendedEvents(state, currentSlideTimestamp, collector);
                currentSlideTimestamp = currentEventTimestamp;

                // Register timers to ensure state cleanup
                long cleanupTime = currentSlideTimestamp + windowSize - windowSlide;
                context.timerService().registerEventTimeTimer(cleanupTime);
            }
            // Add event to state
            addToState(state, currentSlideTimestamp, internalStream);

//            collector.collect(internalStream);
//            System.out.println(internalStream);
//            int size = 0;
//            for(Long key : state.keys()){
//                size++;
//                //System.out.println(key+" : "+state.get(key).size());
//            }
//            System.out.println("window size: "+size);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<InternalStream> out) throws Exception {
            evictOutOfScopeElementsFromWindow(timestamp, out);
        }

        private void evictOutOfScopeElementsFromWindow(Long threshold, Collector<InternalStream> out){
            try {
                Iterator<Long> keys = state.keys().iterator();
                while (keys.hasNext()) {
                    Long stateEventTime = keys.next();
                    if (stateEventTime + windowSize < threshold) {
                        collectEvictedEvents(state, stateEventTime, out);
                        keys.remove();
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        private void collectAppendedEvents(MapState<Long, List<InternalStream>> state, Long key, Collector<InternalStream> out) {
            try {
                if(state.get(key) != null) {
                    for (InternalStream event : state.get(key))
                        out.collect(event);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        private void collectEvictedEvents(MapState<Long, List<InternalStream>> state, Long key, Collector<InternalStream> out) {
            try {
                for(InternalStream event : state.get(key)){
                    ((Input)event).setTimestamp(key);
                    ((Input)event).setVal(-1.0);
                    out.collect(event);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void addToState(MapState<Long, List<InternalStream>> state, Long key, InternalStream value) throws Exception {
            List<InternalStream> currentSlide = state.get(key);
            if (currentSlide == null)
                currentSlide = new ArrayList<>();
            ((Input)value).setTimestamp(key); // assign the events the timestamp of the slide
            currentSlide.add(value);
            state.put(key, currentSlide);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>(
                            "window-state",
                            Types.LONG, Types.LIST(Types.GENERIC(InternalStream.class))));
        }
    }

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

}
