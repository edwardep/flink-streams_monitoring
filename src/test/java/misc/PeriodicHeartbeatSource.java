package misc;

import configurations.AGMSConfig;
import datatypes.InternalStream;
import datatypes.internals.EmptyStream;
import datatypes.internals.Heartbeat;
import datatypes.internals.Input;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.WorldCupMapSource;
import utils.MaxWatermark;

import java.io.IOException;
import java.util.HashMap;

import static kafka.KafkaUtils.createConsumerInput;
import static utils.DefJobParameters.defWorkers;

public class PeriodicHeartbeatSource {

    @Test
    public void periodicHeartbeatTest() throws Exception {
        ParameterTool parameters = ParameterTool.fromPropertiesFile("/home/edwardep/flink-streams_monitoring/src/main/java/properties/pico.properties");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1L);

        AGMSConfig config = new AGMSConfig(parameters);
        SingleOutputStreamOperator<InternalStream> PHB = env
                .addSource(new SourceFunction<InternalStream>() {
                    private boolean isRunning = true;
                    @Override
                    public void run(SourceContext<InternalStream> sourceContext) throws Exception {
                        while(isRunning) {
                            for(int i = 0; i < config.workers(); i++)
                                sourceContext.collect(new Heartbeat(String.valueOf(i)));
                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }
                })
                .assignTimestampsAndWatermarks(new MaxWatermark());

        SingleOutputStreamOperator<InternalStream> streamFromFile = env
                .addSource(
                        createConsumerInput(parameters)
                                .setStartFromEarliest()
                                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                                    @Override
                                    public long extractAscendingTimestamp(String s) {
                                        return Long.parseLong(s.split(";")[0])*1000;
                                    }
                                }))
                .setParallelism(1)
                .flatMap(new WorldCupMapSource(config))
                .setParallelism(1)
                .connect(PHB)
                .keyBy(InternalStream::getStreamID, InternalStream::getStreamID)
                .process(new KeyedCoProcessFunction<String,InternalStream, InternalStream, InternalStream>() {

                    @Override
                    public void processElement1(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                        System.out.println(context.getCurrentKey()+"> "+context.timerService().currentWatermark());
                    }

                    @Override
                    public void processElement2(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                        System.out.println(context.getCurrentKey()+"> "+context.timerService().currentWatermark());
                    }
                });



        env.execute();
    }




    @Test
    public void sourceHashingByK_test() throws Exception {

        ParameterTool parameters = ParameterTool.fromPropertiesFile("/home/edwardep/flink-streams_monitoring/src/main/java/properties/pico.properties");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameters.getInt("workers", defWorkers));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1);
        //env.getConfig().disableGenericTypes(); //todo: Generics fall back to Kryo

        /**
         *  The FGM configuration class. (User-implemented functions)
         */
        AGMSConfig config = new AGMSConfig(parameters);
        SingleOutputStreamOperator<InternalStream> streamFromFile = env
                .addSource(
                        createConsumerInput(parameters)
                                .setStartFromEarliest()
                                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                                    @Override
                                    public long extractAscendingTimestamp(String s) {
                                        return Long.parseLong(s.split(";")[0])*1000;
                                    }
                                }))
                .setParallelism(1)
                .flatMap(new WorldCupMapSource(config))
                .setParallelism(1);

        streamFromFile
                .flatMap(new FlatMapFunction<InternalStream, String>() {
                    HashMap<String, Integer> recordsByServer = new HashMap<>();
                    int count;
                    @Override
                    public void flatMap(InternalStream internalStream, Collector<String> collector) throws Exception {
                        String key = internalStream.getStreamID();
                        recordsByServer.put(key, recordsByServer.getOrDefault(key,0) + 1);
                        count++;
                        if(count > 6999998)
                            collector.collect(recordsByServer.toString());
                    }
                })
                .setParallelism(1)
                .print();

        env.execute();
    }
}
