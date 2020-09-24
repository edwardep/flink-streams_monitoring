package jobs;

import configurations.AGMSConfig;
import datatypes.InternalStream;
import datatypes.internals.InitCoordinator;
import operators.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.OutputTag;
import sources.WorldCupMapSource;
import utils.Misc;

import static utils.DefJobParameters.*;


public class MonitoringJob {

    public static final OutputTag<String> Q_estimate = new OutputTag<String>("estimate-side-output"){};
    public static final OutputTag<InternalStream> feedback = new OutputTag<InternalStream>("feedback-side-input"){};


    public static void main(String[] args) throws Exception {

        /*
            /usr/local/flink/bin/flink run -c jobs.MonitoringJob flink-fgm-1.0.jar \
            --input "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_day46_1.txt" \
            --output "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_part1_test_03.txt" \
            --parallelism 4 \
            --jobName "fgm-w3600-s5-wwu" \
            --window 3600 \
            --slide 5\
            --warmup 5
         */

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(parameters.getInt("parallelism", defParallelism));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        /**
         *  The FGM configuration class. (User-implemented functions)
         */
        AGMSConfig config = new AGMSConfig(parameters.getInt("workers", defWorkers), parameters.getDouble("epsilon", defEpsilon));

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
        DataStream<InternalStream> streamFromFile = env
                .readTextFile(parameters.get("input", defInputPath))
                .flatMap(new WorldCupMapSource(config));

        /**
         *  Creating Iterative Stream
         */
        IterativeStream.ConnectedIterativeStreams<InternalStream, InternalStream > iteration = streamFromFile
                .iterate()
                .withFeedbackType(InternalStream.class);


        /**
         *  This is the iteration Head. It merges the input and the feedback streams and forwards them to the main
         *  and the side output respectively.
         */
        SingleOutputStreamOperator<InternalStream> iteration_input = iteration
                .process(new IterationHead());


        /**
         *  Ascending Timestamp assigner & Sliding Window operator
         */
        SingleOutputStreamOperator<InternalStream> worker = iteration_input
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
//                    @Override
//                    public long extractAscendingTimestamp(InputRecord inputRecord) {
//                        return inputRecord.getTimestamp();
//                    }
//                })
                .keyBy(InternalStream::getStreamID)
//                .timeWindow(
//                        Time.seconds(parameters.getInt("window", defWindowSize)),
//                        Time.seconds(parameters.getInt("slide", defSlideSize)))
//                .aggregate(
//                        new IncAggregation<>(config),
//                        new WindowFunction<>(config),
//                        config.getAccType(),                        // AggregateFunction ACC type
//                        config.getAccType(),                        // AggregateFunction V type
//                        TypeInformation.of(InternalStream.class))   // WindowFunction R type

                /**
                 * The KeyedCoProcessFunction contains all of fgm's worker logic.
                 * Input1 -> Sliding Window output
                 * Input2 -> Feedback stream from IterationHead side-output
                 * Output -> Connects to Coordinator's Input1
                 */
                .connect(iteration_input.getSideOutput(feedback))
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
        Misc.printExecutionMessage(parameters);
        env.execute(parameters.get("jobName", defJobName));
    }
}
