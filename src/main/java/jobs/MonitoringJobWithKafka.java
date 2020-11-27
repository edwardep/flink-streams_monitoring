package jobs;

import configurations.AGMSConfig;
import datatypes.InternalStream;
import datatypes.internals.InitCoordinator;
import datatypes.internals.Input;
import operators.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import sources.WorldCupMapSource;
import utils.MaxWatermark;
import utils.Misc;
import utils.ProcessingTimeTrailingBoundedOutOfOrdernessTimestampExtractor;


import java.util.Properties;

import static kafka.KafkaUtils.*;
import static utils.DefJobParameters.*;


public class MonitoringJobWithKafka {

    public static final OutputTag<String> Q_estimate = new OutputTag<String>("estimate-side-output") {};
    public static final OutputTag<String> localThroughput = new OutputTag<String>("local-throughput-output") {};
    public static final OutputTag<String> coordinatorMetrics = new OutputTag<String>("coordinator-metrics-output") {};
    public static final OutputTag<String> workerMetrics = new OutputTag<String>("worker-metrics-output") {};

    public static boolean endOfFile = false;


    public static void main(String[] args) throws Exception {

        ParameterTool parameters;

        if(args == null || args.length == 0){
            System.err.println("The path to the properties file should be passed as argument");
            return;
        }
        else{
             parameters = ParameterTool.fromPropertiesFile(args[0]);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameters.getInt("workers", defWorkers));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1);


        /**
         *  The FGM configuration class. (User-implemented functions)
         */
        AGMSConfig config = new AGMSConfig(parameters);

        /**
         *  Dummy Source to Initialize coordinator
         */
        SingleOutputStreamOperator<InternalStream> coordinator_init = env
                .addSource(new SourceFunction<InternalStream>() {
                    @Override
                    public void run(SourceContext<InternalStream> sourceContext) throws Exception {
                        sourceContext.collect(new InitCoordinator(897429601000L));
                    }

                    @Override
                    public void cancel() {
                    }
                })
                .setParallelism(1)
                .name("coord_init")
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                    @Override
                    public long extractAscendingTimestamp(InternalStream internalStream) {
                        return ((InitCoordinator) internalStream).getWarmup();
                    }
                });



        /**
         *  Reading line by line from file and streaming POJOs
         */
        SingleOutputStreamOperator<InternalStream> streamFromFile = env
                .addSource(createConsumerInput(parameters).setStartFromEarliest())
                .setParallelism(1)
                .flatMap(new WorldCupMapSource(config))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                    @Override
                    public long extractAscendingTimestamp(InternalStream internalStream) {
                        return ((Input)internalStream).getTimestamp();
                    }
                });

        /**
         *  Creating Iterative Stream
         */
        DataStream<InternalStream> iteration = env
                .addSource(createConsumerInternal(parameters, config))
                .assignTimestampsAndWatermarks(new MaxWatermark())
                .name("Iteration Src");


        SingleOutputStreamOperator<InternalStream> input;

        if(config.slidingWindowEnabled()) {
            /**
             *  Ascending Timestamp assigner & Sliding Window operator
             */
            input = streamFromFile
                    .keyBy(InternalStream::getStreamID)
                    .process(new CustomSlidingWindow(config.windowSize(), config.windowSlide()))
                    .name("SlidingWindow");

        }
        else {
            input = streamFromFile;
        }


        /**
         * The KeyedCoProcessFunction contains all of fgm's worker logic.
         * Input1 -> Sliding Window output
         * Input2 -> Feedback stream from IterationHead side-output
         * Output -> Connects to Coordinator's Input1
         */
        SingleOutputStreamOperator<InternalStream> worker = input
                .connect(iteration)
                .keyBy(InternalStream::getStreamID, InternalStream::getStreamID)
                .process(new WorkerProcessFunction<>(config))
                .name("Workers");
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
                .addSink(createProducerInternal(parameters))
                .name("Iteration Sink");

        /**
         *  Writing output (round_timestamp, Q(E)) to text file
         */
        coordinator
                .getSideOutput(Q_estimate)
                .writeAsText("logs/final/output_"+parameters.get("jobName", defOutputPath), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("Output");


        /**
         * Extract metrics for each worker and the coordinator
         */
        worker
                .getSideOutput(localThroughput)
                .writeAsText("logs/final/throughput_"+parameters.get("jobName"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("Throughput Metrics");

        worker
                .getSideOutput(workerMetrics)
                .writeAsText("logs/final/workerMetrics_"+parameters.get("jobName"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("Worker Metrics");

        coordinator
                .getSideOutput(coordinatorMetrics)
                .writeAsText("logs/final/coordinatorMetrics_"+parameters.get("jobName"), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("Coordinator Metrics");


        //System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute(parameters.get("jobName", defJobName));
    }
}
