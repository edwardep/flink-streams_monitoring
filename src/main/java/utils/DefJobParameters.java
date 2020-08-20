package utils;

public class DefJobParameters {

    public static String defInputTopic = "input";
    public static String defFeedbackTopic = "feedback";
    public static String defKafkaServers = "localhost:9092";

    public static int defParallelism = 4; // Flink Parallelism
    public static int defWindowSize = 1000; //  the size of the sliding window in seconds
    public static int defSlideSize = 5; //  the sliding interval in milliseconds

    public static int defWarmup = 5;  //  warmup duration in seconds (processing time)

    //String defInputPath = "hdfs://clu01.softnet.tuc.gr:8020/user/eepure/wc_day46_1.txt";
    public static String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
    //String defInputPath = "C:/Users/eduar/IdeaProjects/flink-fgm/logs/SyntheticDataSet2.txt";
    //String defOutputPath = "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/outputSk.txt";
    public static String defOutputPath = "C:/Users/eduar/IdeaProjects/flink-streams_monitoring/logs/outputSk_15m_4.txt";
    public static String defJobName = "FGM-pipeline";
}
