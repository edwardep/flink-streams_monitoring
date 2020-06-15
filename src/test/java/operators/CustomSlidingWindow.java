package operators;

import configurations.BaseConfig;
import configurations.FgmConfig;
import configurations.TestP1Config;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.SyntheticEventTimeSource;
import sources.WorldCupSource;
import sources.WorldCupSource2;
import state.WindowStateHandler;
import test_utils.SyntheticSlidingSource;
import utils.DataSetTransformer;

import java.io.FileWriter;
import java.io.IOException;

import static datatypes.InternalStream.windowSlide;

public class CustomSlidingWindow {

    @Test
    public void compareWindows_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
                .keyBy(InputRecord::getStreamID);


        keyedStream
                .timeWindow(Time.seconds(slide))
                .process(new SlideProcess<>(cfg))
                .keyBy(InternalStream::getStreamID)
                .process(new WindowProcess<>(window, slide, cfg))
                .keyBy(InternalStream::getStreamID)
                .process(new ProcessFunction<InternalStream, String>() {
                    Vector state = new Vector();
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        //state = cfg.addVectors(state, (Vector) internalStream.getVector());
                        collector.collect(cfg.queryFunction((Vector) internalStream.getVector(), internalStream.getTimestamp()));
                        //System.out.println(windowSlide("0", internalStream.getTimestamp()+1, internalStream.getVector(),0).toString());
                        //collector.collect(windowSlide("0", internalStream.getTimestamp(), internalStream.getVector(), ((Vector) internalStream.getVector()).map().size()));

                        //writeToText(cfg.queryFunction((Vector) internalStream.getVector(), internalStream.getTimestamp())+"\n", "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/CW05.txt");
                        //writeToText(windowSlide("0", internalStream.getTimestamp(), internalStream.getVector()+"\n", ((Vector) internalStream.getVector()).map().size()).toString(), "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/CW06.txt");
                    }
                });
                //.writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/CW06.txt", FileSystem.WriteMode.OVERWRITE);

//        keyedStream
//                .timeWindow(Time.seconds(window), Time.seconds(slide))
//                .process(new ProcessWindowFunction<InputRecord, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<InputRecord> iterable, Collector<String> collector) throws Exception {
//                        Vector vec = cfg.batchUpdate(iterable);
//                        collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
//                        //System.out.println("FLINK: "+windowSlide("0", context.window().getEnd(), vec,0).toString());
//                        //collector.collect(windowSlide("0", context.window().getEnd(), vec, vec.map().size()));
//
//                        //writeToText(cfg.queryFunction(vec, context.window().getEnd())+"\n", "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/BW05.txt");
//                        //writeToText(windowSlide("0", context.window().getEnd(), vec, vec.map().size()).toString()+"\n", "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/BW06.txt");
//                    }
//                });
                //.writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/BW06.txt", FileSystem.WriteMode.OVERWRITE);


        env.execute();
    }
    private static void writeToText(String string, String filename) {
        try {
            FileWriter myWriter = new FileWriter(filename, true);
            myWriter.write(string);
            myWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }

    @Test
    public void worldCup_flink_window_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int slide = 5;
        int window = 1000;
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";

        //TestP1Config cfg = new TestP1Config();
        FgmConfig cfg = new FgmConfig();


        KeyedStream<InputRecord, String> keyedStream = env
                .addSource(new WorldCupSource(defInputPath))
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(InputRecord::getStreamID);

        keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .process(new ProcessWindowFunction<InputRecord, String, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<InputRecord> iterable, Collector<String> collector) throws Exception {
                        Vector vec = cfg.batchUpdate(iterable);
                        collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
                    }
                });

        env.execute();
    }

    @Test
    public void worldCup_custom_window_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int slide = 5;
        int window = 1000;
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";

        FgmConfig cfg = new FgmConfig();

        KeyedStream<InputRecord, String> keyedStream = env
                .addSource(new WorldCupSource(defInputPath))
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(InputRecord::getStreamID);

        keyedStream
                .timeWindow(Time.seconds(slide))
                .process(new SlideProcess<>(cfg))
                .keyBy(InternalStream::getStreamID)
                .process(new WindowProcess<>(window, slide, cfg))
                .keyBy(InternalStream::getStreamID)
                .process(new KeyedProcessFunction<String, InternalStream, String>() {
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        collector.collect(cfg.queryFunction((Vector) internalStream.getVector(), internalStream.getTimestamp()));
                    }
                });

        env.execute();
    }

    @Test
    public void slidingSource_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
                .keyBy(InputRecord::getStreamID);

        keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .process(new ProcessWindowFunction<InputRecord, String, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<InputRecord> iterable, Collector<String> collector) throws Exception {
                        Vector vec = cfg.batchUpdate(iterable);
                        collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
                    }
                })
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/Synthetic01.txt", FileSystem.WriteMode.OVERWRITE);

        KeyedStream<InputRecord, String> keyedStream2 = env
                .addSource(new SyntheticSlidingSource(window))
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(InputRecord::getStreamID);

        keyedStream2
                .timeWindow(Time.seconds(slide))
                .process(new SlideProcess<>(cfg))
                .process(new ProcessFunction<InternalStream, String>() {
                    Vector vec = new Vector();
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        vec = cfg.addVectors(vec, (Vector) internalStream.getVector());
                        collector.collect(cfg.queryFunction(vec, internalStream.getTimestamp()));
                    }
                })
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/Sliding01.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    @Test
    public void slidingWorldCup_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int slide = 5;
        int window = 1000;
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1_slide.txt";

        //TestP1Config cfg = new TestP1Config();
        FgmConfig cfg = new FgmConfig();

        KeyedStream<InputRecord, String> keyedStream = env
                .addSource(new WorldCupSource2(defInputPath))
               // .addSource(new SyntheticSlidingSource(window))
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(InputRecord::getStreamID);

        keyedStream
                .timeWindow(Time.seconds(slide))
                .process(new SlideProcess<>(cfg))
                .process(new ProcessFunction<InternalStream, String>() {
                    Vector vec = new Vector();
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        vec = cfg.addVectors(vec, (Vector) internalStream.getVector());
                        collector.collect(cfg.queryFunction(vec, internalStream.getTimestamp()));
                    }
                });

        env.execute();
    }

    @Test
    public void dataSetTrans_test() throws IOException {

        DataSetTransformer transformer = new DataSetTransformer();
    }

    @Test
    public void compareWindowsAgg_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

//        keyedStream
//                .timeWindow(Time.seconds(window), Time.seconds(slide))
//                .process(new ProcessWindowFunction<InputRecord, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String s, Context context, Iterable<InputRecord> iterable, Collector<String> collector) throws Exception {
//                        collector.collect(cfg.queryFunction(cfg.batchUpdate(iterable), context.window().getEnd()));
//                    }
//                })
//                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/simpleWindow.txt", FileSystem.WriteMode.OVERWRITE);

        keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new WinAgg(), new WindowFunc())
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/aggWindow.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    public static class WinAgg implements AggregateFunction<InputRecord, Vector, Vector> {


        @Override
        public Vector createAccumulator() {
            return new Vector();
        }

        @Override
        public Vector add(InputRecord inputRecord, Vector vector) {
            vector.map().put(inputRecord.getKey(), vector.getValue(inputRecord.getKey()) + inputRecord.getVal());
            return vector;
        }

        @Override
        public Vector getResult(Vector vector) {
            return vector;
        }

        @Override
        public Vector merge(Vector vector, Vector acc1) {
            return null;
        }
    }

    public static class WindowFunc extends ProcessWindowFunction<Vector, String, String, TimeWindow> {

        private BaseConfig<Vector,?> cfg = new TestP1Config();

        @Override
        public void process(String s, Context context, Iterable<Vector> iterable, Collector<String> collector) throws Exception {
            Vector vec = iterable.iterator().next();

            collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
        }
    }

}
