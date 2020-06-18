package operators;

import configurations.BaseConfig;
import configurations.TestP1Config;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.Test;
import sources.SyntheticEventTimeSource;
import sources.WorldCupSource;

import static datatypes.InternalStream.initializeCoordinator;

public class CustomOperator {



    @Test
    public void createCustomOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        MonitoringOperator<String, Integer> monitoring = new MonitoringOperator<>();

        DataStream<String> source = env.fromElements("11");

        monitoring
                .addSource(source)
                .execute()
                .print();



        env.execute();
    }


    public static class MonitoringOperator<IN, OUT> {

        DataStream<IN> source;
        
        public MonitoringOperator<IN, OUT> addSource(DataStream<IN> source){
            this.source = source;
            return this;
        }



        public DataStream<OUT> execute() {

            return source
                    .map((MapFunction<IN, OUT>) in -> (OUT) String.valueOf(Integer.parseInt((String) in)*2));
        }

    }


}
