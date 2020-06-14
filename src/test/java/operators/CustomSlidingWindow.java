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
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.WorldCupSource;
import test_utils.SmartSource;
import test_utils.SyntheticEventTimeSource;
import test_utils.TestP1Config;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
                .process(new SlideAggregate<>(cfg))  // this does not trigger every 5 seconds ONLY if it has new records
                .keyBy(InternalStream::getStreamID)
                .process(new WindowAggregate<>(window, slide, cfg))
                .keyBy(InternalStream::getStreamID)
                .process(new ProcessFunction<InternalStream, String>() {
                    Vector state = new Vector();
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        //state = cfg.addVectors(state, (Vector) internalStream.getVector());
                        collector.collect(cfg.queryFunction((Vector) internalStream.getVector(), internalStream.getTimestamp()));
                        //System.out.println(windowSlide("0", internalStream.getTimestamp()+1, internalStream.getVector(),0).toString());
                        //collector.collect(windowSlide("0", internalStream.getTimestamp(), internalStream.getVector(), ((Vector) internalStream.getVector()).map().size()));

                        writeToText(cfg.queryFunction((Vector) internalStream.getVector(), internalStream.getTimestamp())+"\n", "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/CW05.txt");
                        writeToText(windowSlide("0", internalStream.getTimestamp(), internalStream.getVector()+"\n", ((Vector) internalStream.getVector()).map().size()).toString(), "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/CW06.txt");
                    }
                });
                //.writeAsText("C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/CW06.txt", FileSystem.WriteMode.OVERWRITE);

        keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .process(new ProcessWindowFunction<InputRecord, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<InputRecord> iterable, Collector<String> collector) throws Exception {
                        Vector vec = cfg.batchUpdate(iterable);
                        //collector.collect(cfg.queryFunction(vec, context.window().getEnd()));
                        //System.out.println("FLINK: "+windowSlide("0", context.window().getEnd(), vec,0).toString());
                        //collector.collect(windowSlide("0", context.window().getEnd(), vec, vec.map().size()));

                        writeToText(cfg.queryFunction(vec, context.window().getEnd())+"\n", "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/BW05.txt");
                        writeToText(windowSlide("0", context.window().getEnd(), vec, vec.map().size()).toString()+"\n", "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/BW06.txt");
                    }
                });
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
}
