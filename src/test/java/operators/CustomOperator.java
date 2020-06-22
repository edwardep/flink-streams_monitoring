package operators;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;


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
