package operators;

import akka.protobuf.Internal;
import configurations.FgmConfig;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.WorldCupSource;
import test_utils.SyntheticEventTimeSource;
import test_utils.TestP1Config;

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
                .process(new SlideAggregate<>(cfg))
                .keyBy(InternalStream::getStreamID)
                .process(new WindowAggregate<>(window, slide, cfg))
                .process(new ProcessFunction<InternalStream, String>() {
                    Vector state = new Vector();
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        state = cfg.addVectors(state, (Vector) internalStream.getVector());
                        collector.collect(cfg.queryFunction(state, internalStream.getTimestamp()+1));
                        //collector.collect(windowSlide("0", internalStream.getTimestamp()+1, state));
                    }
                })
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/CW03.txt", FileSystem.WriteMode.OVERWRITE);

        keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .process(new ProcessWindowFunction<InputRecord, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<InputRecord> iterable, Collector<String> collector) throws Exception {
                        Vector vec = cfg.batchUpdate(iterable);
                        collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
                        //collector.collect(windowSlide("0", context.window().getEnd(), vec));
                    }
                })
                .writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/BW03.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }
}
