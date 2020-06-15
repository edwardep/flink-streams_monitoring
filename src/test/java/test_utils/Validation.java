package test_utils;

import configurations.TestP4Config;
import datatypes.InputRecord;
import datatypes.Vector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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
import sources.SyntheticEventTimeSource;
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

        TestP4Config cfg = new TestP4Config();

        KeyedStream<InputRecord, String> keyedStream = env
                .addSource(new SyntheticEventTimeSource())
                //.addSource(new WorldCupSource(defInputPath))
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
                .process(new ProcessWindowFunction<InputRecord, String, String, TimeWindow>() {

                    @Override
                    public void process(String s, Context context, Iterable<InputRecord> iterable, Collector<String> collector) throws Exception {
                        Vector vec = cfg.scaleVector(cfg.batchUpdate(iterable),1/4d);
                        collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
                    }
                })
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/validation_1000.txt", FileSystem.WriteMode.OVERWRITE);



        env.execute();
    }
}
