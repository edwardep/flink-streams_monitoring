package jobs;

import configurations.AGMSConfig;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.internals.InitCoordinator;
import operators.CoordinatorProcessFunction;
import operators.IncAggregation;
import operators.WindowFunction;
import operators.WorkerProcessFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sources.WorldCupMapSource;
import sources.WorldCupSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Random;

import static kafka.KafkaUtils.createConsumerInternal;
import static kafka.KafkaUtils.createProducerInternal;


public class MonitoringJobWithKafka {

    private static final OutputTag<String> Q_estimate = new OutputTag<String>("estimate-side-output"){};


    public static void main(String[] args) throws Exception {

        /*
            /usr/local/flink/bin/flink run -c jobs.MonitoringJob flink-fgm-1.0.jar \
            --input "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_day46_1.txt" \
            --output "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_part1_test_03.txt" \
            --p 4 \
            --jobName "fgm-w3600-s5-wwu" \
            --window 3600 \
            --slide 5\
            --warmup 5
         */


        int defParallelism = 4; // Flink Parallelism
        int defWindowSize = 3000; //  the size of the sliding window in seconds
        int defSlideSize = 5; //  the sliding interval in milliseconds

        int defWarmup = 5;  //  warmup duration in seconds (processing time)

        //String defInputPath = "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_day46_1.txt";
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
        //String defInputPath = "C:/Users/eduar/IdeaProjects/flink-fgm/logs/SyntheticDataSet2.txt";
        //String defOutputPath = "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/outputSk.txt";
        String defOutputPath = "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/outputSk_15m_4.txt";
        String defJobName = "FGM-pipeline";


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(parameters.getInt("p", defParallelism));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5);

        /**
         *  Kafka Configuration
         */
        Random rand = new Random();
        String feedbackTopic = "feedback";
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "group"+rand.nextLong());

        Properties producerProps = new Properties();
        producerProps.setProperty("bootstrap.servers", "localhost:9092");


        /**
         *  The FGM configuration class. (User-implemented functions)
         */
        //FgmConfig config = new FgmConfig();
        //TestP1Config config = new TestP1Config();
        //TestP4Config config = new TestP4Config();
        AGMSConfig config = new AGMSConfig();

        /**
         *  Dummy Source to Initialize coordinator
         */
        SingleOutputStreamOperator<InternalStream> coordinator_init = env
                .addSource(new SourceFunction<InternalStream>() {
                    @Override
                    public void run(SourceContext<InternalStream> sourceContext) throws Exception {
                        sourceContext.collect(new InitCoordinator(parameters.getInt("warmup", defWarmup)));
                    }
                    @Override
                    public void cancel() { }
                })
                .setParallelism(1)
                .name("coord_init");


        /**
         *  Reading line by line from file and streaming POJOs
         */
        DataStream<InputRecord> streamFromFile = env
                .readTextFile(parameters.get("input", defInputPath))
                .flatMap(new WorldCupMapSource(config))
                .process(new ProcessFunction<InputRecord, InputRecord>() {
                    @Override
                    public void processElement(InputRecord inputRecord, Context context, Collector<InputRecord> collector) throws Exception {
                        collector.collect(inputRecord);
                    }
                });

//        DataStream<InputRecord> streamFromFile = env
//                .addSource(new WorldCupSource(defInputPath, config))
//                .map(x -> x)
//                .returns(TypeInformation.of(InputRecord.class));

        /**
         *  Creating Iterative Stream
         */
        DataStream<InternalStream> iteration = env
                .addSource(createConsumerInternal(feedbackTopic, consumerProps))
                .name("Iteration Src");


        /**
         *  Ascending Timestamp assigner & Sliding Window operator
         */
        SingleOutputStreamOperator<InternalStream> worker = streamFromFile
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(InputRecord::getStreamID)
                .timeWindow(
                        Time.seconds(parameters.getInt("window", defWindowSize)),
                        Time.seconds(parameters.getInt("slide", defSlideSize)))
                .aggregate(
                        new IncAggregation<>(config),
                        new WindowFunction<>(config),
                        config.getAccType(),                        // AggregateFunction ACC type
                        config.getAccType(),                        // AggregateFunction V type
                        TypeInformation.of(InternalStream.class))   // WindowFunction R type

                /**
                 * The KeyedCoProcessFunction contains all of fgm's worker logic.
                 * Input1 -> Sliding Window output
                 * Input2 -> Feedback stream from IterationHead side-output
                 * Output -> Connects to Coordinator's Input1
                 */
                .connect(iteration)
                .keyBy(InternalStream::getStreamID, InternalStream::getStreamID)
                .process(new WorkerProcessFunction<>(config));

        /**
         *  FGM Coordinator, a KeyedCoProcessFunction with:
         *  Input1 -> output of Workers
         *  Input2 -> dummy stream to start the protocol
         *  Output -> feedback broadcast
         *  Side-output -> Q(E)
         */
        SingleOutputStreamOperator<InternalStream> coordinator = worker
                .connect(coordinator_init)
                .keyBy(InternalStream::unionKey, InternalStream::unionKey)
                .process(new CoordinatorProcessFunction<>(config))
                .setParallelism(1)
                .name("coordinator");


        /**
         *  Re-balancing feedback stream to match system parallelism (iterative stream requirement)
         */
        DataStream<InternalStream> feedback = coordinator
                .map(x -> x)
                .returns(TypeInformation.of(InternalStream.class))
                .name("broadcast");


        /**
         *  closing iteration with feedback stream
         */
        feedback
                .addSink(createProducerInternal(feedbackTopic, producerProps))
                .name("Iteration Sink");

        /**
         *  Writing output (round_timestamp, Q(E)) to text file
         */
        coordinator
                .getSideOutput(Q_estimate)
                .writeAsText(parameters.get("output", defOutputPath), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("Output");


        System.out.println(env.getExecutionPlan());
        env.execute(parameters.get("jobName", defJobName));
    }
}
