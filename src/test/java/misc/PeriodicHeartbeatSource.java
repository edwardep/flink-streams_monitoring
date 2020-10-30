package misc;

import configurations.AGMSConfig;
import datatypes.InternalStream;
import datatypes.internals.EmptyStream;
import datatypes.internals.Heartbeat;
import datatypes.internals.InitCoordinator;
import datatypes.internals.Input;
import operators.CustomSlidingWindow;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.WorldCupMapSource;
import utils.MaxWatermark;

import javax.print.attribute.IntegerSyntax;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

import static kafka.KafkaUtils.createConsumerInput;
import static utils.DefJobParameters.defWorkers;

public class PeriodicHeartbeatSource {

    @Test
    public void singleEventTriggerTimer() throws Exception {
        ParameterTool parameters = ParameterTool.fromPropertiesFile("/home/edwardep/flink-streams_monitoring/src/main/java/properties/pico.properties");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1L);

        AGMSConfig config = new AGMSConfig(parameters);

        env
                .addSource(new SourceFunction<InitCoordinator>() {
                    @Override
                    public void run(SourceContext<InitCoordinator> sourceContext) throws Exception {
                        sourceContext.collect(new InitCoordinator(897429601));
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InitCoordinator>() {
                    @Override
                    public long extractAscendingTimestamp(InitCoordinator initCoordinator) {
                        return initCoordinator.getWarmup();
                    }
                })
                .keyBy(InternalStream::unionKey)
                .process(new KeyedProcessFunction<String, InitCoordinator, String>() {
                    @Override
                    public void processElement(InitCoordinator initCoordinator, Context context, Collector<String> collector) throws Exception {

                        long cw = context.timerService().currentWatermark();
                        System.out.println(cw + " @ "+context.timestamp());

                        context.timerService().registerEventTimeTimer(initCoordinator.getWarmup() + 20000);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("Timer fired @ "+timestamp);
                    }
                });

        env.execute();
    }

    @Test
    public void idleSourceTest() throws Exception {
        ParameterTool parameters = ParameterTool.fromPropertiesFile("/home/edwardep/flink-streams_monitoring/src/main/java/properties/pico.properties");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1L);

        AGMSConfig config = new AGMSConfig(parameters);

        DataStream<InternalStream> source = env
                .addSource(new SourceFunction<InternalStream>() {
                    private boolean isRunning = true;
                    private int count = 0;
                    private Random rand = new Random(5);
                    @Override
                    public void run(SourceContext<InternalStream> sourceContext) throws Exception {

                        long timestamp = 0;

                        while(isRunning && count++ < 5000){
                            Input event = new Input(
                                    //rand.nextInt(10) < 9 ? "0":"1",
                                    //count < 3 ? "1" : "0",
                                    "0",
                                    timestamp+=50000,
                                    Tuple2.of(0,count),
                                    1.0);

                            sourceContext.collect(event);
                        }
                    }

                    @Override
                    public void cancel() {
                        isRunning = false;
                    }
                })


                .assignTimestampsAndWatermarks(
                        new ProcessingTimeTrailingBoundedOutOfOrdernessTimestampExtractor<InternalStream>(
                                Time.milliseconds(0),           // maxOutOfOrderness
                                Time.milliseconds(2),       // idlenessDetectionDuration
                                Time.milliseconds(2)) {      // processingTimeTrailing
                    @Override
                    public long extractTimestamp(InternalStream element) {
                        return ((Input) element).getTimestamp();
                    }
                });
//                .keyBy(InternalStream::getStreamID)
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
//                    @Override
//                    public long extractAscendingTimestamp(InternalStream internalStream) {
//                        return ((Input) internalStream).getTimestamp();
//                    }
//                });

        source
                .process(new ProcessFunction<InternalStream, String>() {
                    @Override
                    public void processElement(InternalStream internalStream, Context context, Collector<String> collector) throws Exception {
                        System.out.println(context.timerService().currentWatermark()+ " > " + internalStream);
                    }
                });
//        source
//                .keyBy(InternalStream::getStreamID)
//                .process(new CustomSlidingWindow(Time.seconds(20), Time.seconds(2)))
//                .print();


        env.execute();
    }



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
