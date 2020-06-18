package test_utils;

import configurations.BaseConfig;
import configurations.FgmConfig;
import datatypes.InputRecord;
import datatypes.Vector;
import operators.IncAggregation;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import sources.WorldCupSource;


public class Validation {

    @Test
    public void centralized() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";

        int slide = 5;
        int window = 1000;

        //TestP4Config cfg = new TestP4Config();
        FgmConfig cfg = new FgmConfig();

        KeyedStream<InputRecord, String> keyedStream = env
                //.addSource(new SyntheticEventTimeSource())
                .addSource(new WorldCupSource(defInputPath))
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(k->"0");

        IncAggregation<Vector, InputRecord> aggregation = new IncAggregation<>(cfg);
        keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(aggregation, new WindowFunc(cfg))
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/validation_1000.txt", FileSystem.WriteMode.OVERWRITE);



        env.execute();
    }

    public static class WindowAgg implements AggregateFunction<InputRecord, Vector, Vector> {
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

    private static class WindowFunc extends ProcessWindowFunction<Vector, String, String, TimeWindow> {

        private BaseConfig<Vector,?> cfg;

        WindowFunc(BaseConfig<Vector, ?> cfg) { this.cfg = cfg; }

        @Override
        public void process(String s, Context context, Iterable<Vector> iterable, Collector<String> collector) throws Exception {
            Vector vec = cfg.scaleVector(iterable.iterator().next(), 1.0/cfg.getKeyGroupSize());
            collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
        }
    }
}
