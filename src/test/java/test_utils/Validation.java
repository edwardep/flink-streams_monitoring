package test_utils;

import configurations.BaseConfig;
import configurations.TestP1Config;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.Input;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
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

import sources.WorldCupMapSource;
import sources.WorldCupSource;

import java.io.IOException;
import java.util.HashMap;

import static utils.DefJobParameters.defInputPath;


public class Validation {
    private TestP1Config cfg = new TestP1Config();

    @Test
    public void naive() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
        env
                .readTextFile(defInputPath)
                .flatMap(new WorldCupMapSource(cfg))
                .process(new NaiveProcess())
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/validation_windowless.txt", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    private static class NaiveProcess extends ProcessFunction<InternalStream, String> {

        private TestP1Config cfg = new TestP1Config();

        Vector state = new Vector();
        int count = 0;
        @Override
        public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
            Tuple2<Integer, Integer> key = ((Input)internalStream).getKey();
            Double val = ((Input)internalStream).getVal();
            state.map().put(key, state.getValue(key) + val);

            if(count % 1000 == 0)
                collector.collect(cfg.queryFunction(cfg.scaleVector(state, 1.0/10), ((Input)internalStream).getTimestamp()));

            count++;
        }
    }

    @Test
    public void centralized() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";

        int slide = 5;
        int window = 1000;


        KeyedStream<InputRecord, String> keyedStream = env
                .addSource(new WorldCupSource(defInputPath, cfg))
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(k->"0");

        keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new IncAggregationDef(cfg), new WindowFunctionDef(cfg))
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/validation_1000.txt", FileSystem.WriteMode.OVERWRITE);



        env.execute();
    }
    public static class WindowFunctionDef extends ProcessWindowFunction<Vector, String, String, TimeWindow> {

        private BaseConfig<Vector,Vector,?> cfg;

        WindowFunctionDef(BaseConfig<Vector, Vector, ?> cfg) {
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


    public static class IncAggregationDef implements AggregateFunction<InputRecord, Vector, Vector> {
        private BaseConfig<Vector,?,InputRecord> cfg;

        IncAggregationDef(BaseConfig<Vector, ?, InputRecord> cfg) {
            this.cfg = cfg;
        }

        @Override
        public Vector createAccumulator() {
            return new Vector();
        }
        @Override
        public Vector add(InputRecord input, Vector accumulator) {
            return cfg.aggregateRecord(input, accumulator);
        }
        @Override
        public Vector getResult(Vector accumulator) {
            return accumulator;
        }
        @Override
        public Vector merge(Vector acc1, Vector acc2) {
            return null;
        }
    }

}
