package utils;

public class DefJobParameters {

    public static String defInputTopic = "input-testset-5000";
    public static String defFeedbackTopic = "feedback";
    public static String defKafkaServers = "localhost:9092";

    public static int defParallelism = 2; // Flink Parallelism
    public static int defWindowSize = 1000; //  the size of the sliding window in seconds
    public static int defSlideSize = 5; //  the sliding interval in milliseconds

    public static int defWarmup = 0;  //  warmup duration in seconds (processing time)

    //String defInputPath = "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_day46_1.txt";
    public static String defInputPath = "/home/edwardep/Documents/wc_tools/output/test_set_5000.txt";
    //String defInputPath = "C:/Users/eduar/IdeaProjects/flink-fgm/logs/SyntheticDataSet2.txt";
    //String defOutputPath = "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/outputSk.txt";
    public static String defOutputPath = "/home/edwardep/flink-streams_monitoring/logs/output-testset.txt";
    public static String defJobName = "FGM-pipeline";

    public static int defWorkers = 10;
    public static double defEpsilon = 0.2;
}
