package jobs;

import configurations.AGMSConfig;
import datatypes.InternalStream;
import datatypes.internals.InitCoordinator;
import datatypes.internals.Input;
import operators.*;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.OutputTag;
import sources.WorldCupMapSource;
import utils.Misc;

import static kafka.KafkaUtils.createConsumerInput;
import static utils.DefJobParameters.*;


public class MonitoringJob {

    public static final OutputTag<String> Q_estimate = new OutputTag<String>("estimate-side-output"){};
    public static final OutputTag<InternalStream> feedback = new OutputTag<InternalStream>("feedback-side-input"){};


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
        env.setParallelism(parameters.getInt("parallelism", defParallelism));
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
                    public void cancel() { }
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
                .setParallelism(config.workers());



        SingleOutputStreamOperator<InternalStream> input;
        if(config.slidingWindowEnabled()) {
            /**
             *  Ascending Timestamp assigner & Sliding Window operator
             */
            input = streamFromFile
                    .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InternalStream>() {
                        @Override
                        public long extractAscendingTimestamp(InternalStream inputRecord) {
                            return ((Input) inputRecord).getTimestamp();
                        }
                    })
                    .keyBy(InternalStream::getStreamID)
                    .process(new CustomSlidingWindow(config.windowSize(), config.windowSlide()))
                    .setParallelism(config.workers())
                    .name("Sliding Window");

        }
        else {
            input = streamFromFile;
        }

        /**
         *  Creating Iterative Stream
         */
        IterativeStream.ConnectedIterativeStreams<InternalStream, InternalStream> iteration = input
                .iterate()
                .withFeedbackType(InternalStream.class);


        /**
         *  This is the iteration Head. It merges the input and the feedback streams and forwards them to the main
         *  and the side output respectively.
         */
        SingleOutputStreamOperator<InternalStream> iteration_input = iteration
                .process(new IterationHead())
                .setParallelism(config.workers());


        /**
         * The KeyedCoProcessFunction contains all of fgm's worker logic.
         * Input1 -> Sliding Window output
         * Input2 -> Feedback stream from IterationHead side-output
         * Output -> Connects to Coordinator's Input1
         */
        SingleOutputStreamOperator<InternalStream> worker = iteration_input
                .connect(iteration_input.getSideOutput(feedback))
                .keyBy(InternalStream::getStreamID, InternalStream::getStreamID)
                .process(new WorkerProcessFunction<>(config))
                .setParallelism(config.workers())
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
                .setParallelism(config.workers())
                .returns(TypeInformation.of(InternalStream.class))
                .name("broadcast");


        /**
         *  closing iteration with feedback stream
         */
        iteration.closeWith(feedback);

        /**
         *  Writing output (round_timestamp, Q(E)) to text file
         */
        coordinator
                .getSideOutput(Q_estimate)
                .writeAsText(parameters.get("output", defOutputPath), FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
                .name("Output");


        System.out.println(env.getExecutionPlan());
//        Misc.printExecutionMessage(parameters);
        JobExecutionResult executionResult = env.execute(parameters.get("jobName", defJobName));
        Misc.printExecutionResults(parameters, executionResult);
    }
}
