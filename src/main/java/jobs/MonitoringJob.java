package jobs;

import configurations.FgmConfig;
import configurations.TestP1Config;
import configurations.TestP4Config;
import datatypes.InputRecord;
import datatypes.InternalStream;
import operators.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sources.SyntheticEventTimeSource;
import sources.WorldCupSource;

import static datatypes.InternalStream.initializeCoordinator;
import static datatypes.InternalStream.windowSlide;

public class MonitoringJob {

    public static final OutputTag<String> Q_estimate = new OutputTag<String>("estimate-side-output"){};
    public static final OutputTag<InternalStream> feedback = new OutputTag<InternalStream>("feedback-side-input"){};


    public static void main(String[] args) throws Exception {

        int defParallelism = 2; // Flink Parallelism
        int defWindowSize = 1000; //  the size of the sliding window in seconds
        int defSlideSize = 5; //  the sliding interval in milliseconds

        long defWarmup = 3000;  //  warmup duration in milliseconds

        //String defInputPath = "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_day46_1.txt";
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
        //String defInputPath = "C:/Users/eduar/IdeaProjects/flink-fgm/logs/SyntheticDataSet2.txt";
        String defOutputPath = "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/output.txt";
        String defJobName = "FGM-pipeline";


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);
        env.setParallelism(parameters.getInt("p", defParallelism));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        /**
         *  The FGM configuration class. (User-implemented functions)
         */
        //FgmConfig config = new FgmConfig();
        //TestP1Config config = new TestP1Config();
        TestP4Config config = new TestP4Config(0.1);

        /**
         *  Dummy Source to Initialize coordinator
         */
        DataStream<InternalStream> coordinator_init = env
                .fromElements(initializeCoordinator(parameters.getLong("warmup", defWarmup),null))
                .setParallelism(1)
                .name("coord_init");


        /**
         *  Reading line by line from file and streaming POJOs
         */
//        DataStream<InputEvent> streamFromFile = env
//                .readTextFile(parameters.get("input", defInputPath))
//                .flatMap(new WorldCupSourceHDFS());
        DataStream<InputRecord> streamFromFile = env
                .addSource(new SyntheticEventTimeSource())
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class));

        /**
         *  Creating Iterative Stream
         */
        IterativeStream.ConnectedIterativeStreams<InputRecord, InternalStream > iteration = streamFromFile
                .iterate()
                .withFeedbackType(InternalStream.class);


        /**
         *  This is the iteration Head. It merges the input and the feedback streams and forwards them to the main
         *  and the side output respectively.
         */
        SingleOutputStreamOperator<InputRecord> iteration_input = iteration
                .process(new IterationHead());


        /**
         *  Ascending Timestamp assigner & Sliding Window operator
         */
        SingleOutputStreamOperator<InternalStream> worker = iteration_input
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
                .aggregate(new IncAggregation<>(config), new WindowFunction<>())

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
                .keyBy(stream->stream.unionKey(), init->init.unionKey())
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
                .setParallelism(1);


        System.out.println(env.getExecutionPlan());
        env.execute(parameters.get("jobName", defJobName));
    }
}
