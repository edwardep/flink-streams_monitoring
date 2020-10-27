package test_utils;

import configurations.AGMSConfig;
import configurations.BaseConfig;
import configurations.TestP1Config;
import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.Input;
import fgm.WorkerFunction;
import operators.CustomSlidingWindow;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import sketches.AGMSSketch;
import sources.WorldCupMapSource;
import sources.WorldCupSource;
import state.WorkerStateHandler;

import java.io.IOException;

import static kafka.KafkaUtils.createConsumerInput;


public class Validation {
    private TestP1Config cfg = new TestP1Config();

    @Test
    public void naive() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String defInputPath = "/home/edwardep/Documents/wc_tools/output/wc_day46_1.txt";
        env
                .readTextFile(defInputPath)
                .flatMap(new WorldCupMapSource(cfg))
                .process(new NaiveProcess())
                .writeAsText("/home/edwardep/flink-streams_monitoring/logs/validation_windowless.txt", FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult result = env.execute();
        System.out.println("time: "+ result.getNetRuntime());
    }

    private static class NaiveProcess extends ProcessFunction<InternalStream, String> {

        private TestP1Config cfg = new TestP1Config();

        Vector state = new Vector();
        int count = 0;
        @Override
        public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
            Tuple2<Integer, Integer> key = ((Input)internalStream).getKey();
            Double val = ((Input)internalStream).getVal();
            state.put(key, state.getOrDefault(key, 0d) + val);

            if(count % 1000 == 0)
                collector.collect(cfg.queryFunction(cfg.scaleVector(state, 1.0/10), ((Input)internalStream).getTimestamp()));

            count++;
        }
    }

    @Test
    public void windowedValidation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1L);
        ParameterTool parameters = ParameterTool.fromPropertiesFile("/home/edwardep/flink-streams_monitoring/src/main/java/properties/pico.properties");
        AGMSConfig config = new AGMSConfig(parameters);
        env
                .addSource(createConsumerInput(parameters).setStartFromEarliest())
                .flatMap(new WorldCupMapSource(config))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                    @Override
                    public long extractAscendingTimestamp(InternalStream inputRecord) {
                        return ((Input)inputRecord).getTimestamp();
                    }
                })
                .keyBy(InternalStream::getStreamID)
                .process(new CustomSlidingWindow(Time.seconds(14400), Time.seconds(5)))
                .keyBy(InternalStream::getStreamID)
                .process(new KeyedProcessFunction<String, InternalStream, String>() {
                    WorkerStateHandler<AGMSSketch> state;
                    long currentSlideTimestamp = 0L;
                    @Override
                    public void processElement(InternalStream input, Context context, Collector<String> collector) throws Exception {
                        long currentEventTimestamp = ((Input)input).getTimestamp();

                        if(currentEventTimestamp - currentSlideTimestamp >= config.windowSlide().toMilliseconds()) {
                            currentSlideTimestamp = currentEventTimestamp;
                            collector.collect(config.queryFunction(config.scaleVector(state.getDrift(), 1/10.0), ((Input) input).getTimestamp()));
                        }
                        WorkerFunction.updateDrift(state, input, config);
                    }
                    @Override
                    public void open(Configuration parameters) {
                        state = new WorkerStateHandler<>(getRuntimeContext(), config);
                    }
                })
                .writeAsText("/home/edwardep/flink-streams_monitoring/logs/validation_window_4h_fromfile.txt", FileSystem.WriteMode.OVERWRITE);


        System.out.println(env.getExecutionPlan());

        env.execute();
    }
//    @Test
//    public void centralized() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
//
//        int slide = 5;
//        int window = 1000;
//
//
//        KeyedStream<InputRecord, String> keyedStream = env
//                .addSource(new WorldCupSource(defInputPath, cfg))
//                .map(x -> x)
//                .returns(TypeInformation.of(InputRecord.class))
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
//                    @Override
//                    public long extractAscendingTimestamp(InputRecord inputRecord) {
//                        return inputRecord.getTimestamp();
//                    }
//                })
//                .keyBy(k->"0");
//
//        keyedStream
//                .timeWindow(Time.seconds(window), Time.seconds(slide))
//                .aggregate(new IncAggregationDef(cfg), new WindowFunctionDef(cfg))
//                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/validation_1000.txt", FileSystem.WriteMode.OVERWRITE);
//
//
//
//        env.execute();
//    }
    public static class WindowFunctionDef extends ProcessWindowFunction<Vector, String, String, TimeWindow> {

        private BaseConfig<Vector> cfg;

        WindowFunctionDef(BaseConfig<Vector> cfg) {
            this.cfg = cfg;
        }

        @Override
        public void process(String key, Context ctx, Iterable<Vector> iterable, Collector<String> out) throws IOException {
            if(iterable.iterator().hasNext()){
                Vector vec = iterable.iterator().next();
                out.collect(cfg.queryFunction(cfg.scaleVector(vec, 1.0/10),ctx.window().getEnd()));
            }
        }
    }


}
