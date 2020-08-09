package misc;

import datatypes.InternalStream;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Ignore;
import org.junit.Test;
import java.io.Serializable;

import java.util.Objects;
import java.util.Properties;

public class KafkaIteration {

    @Ignore
    @Test
    public void kafka_topic_test() throws Exception {
        String inputTopic = "flink_input";
        String outputTopic = "flink_output";

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "feedback");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        final OutputTag<Tuple2<Integer, Integer>> outputTag = new OutputTag<Tuple2<Integer, Integer>>("output"){};

        DataStream<MyCustomObject> feedback = env
                .addSource(new FlinkKafkaConsumer<>(
                        outputTopic,
                        new TypeInformationSerializationSchema<>(TypeInformation.of(MyCustomObject.class), env.getConfig()),
                        props)
                        .setStartFromLatest());


        SingleOutputStreamOperator<MyCustomObject> process = env
                .fromElements(new MyCustomObject("key0", 0))
                .connect(feedback)
                .keyBy(k->k.key, l->l.key)
                .process(new KeyedCoProcessFunction<String, MyCustomObject, MyCustomObject, MyCustomObject>() {
                    private int sum = 0;
                    private int iteration = 0;
                    @Override
                    public void processElement1(MyCustomObject myCustomObject, Context context, Collector<MyCustomObject> collector) throws Exception {
                        collector.collect(new MyCustomObject(myCustomObject.getKey(), myCustomObject.getValue()));
                    }

                    @Override
                    public void processElement2(MyCustomObject myCustomObject, Context context, Collector<MyCustomObject> collector) throws Exception {
                        iteration++;
                        if(sum % 5 == 0)
                            context.output(outputTag, Tuple2.of(iteration, sum));
                        sum += myCustomObject.getValue();
                        collector.collect(new MyCustomObject(myCustomObject.getKey(), 1));
                    }
                });

        process.addSink(new FlinkKafkaProducer<>(
                        "localhost:9092",
                        outputTopic,
                        new TypeInformationSerializationSchema<>(TypeInformation.of(MyCustomObject.class), env.getConfig())));

        process.getSideOutput(outputTag)
                .print();

        env.execute();
    }


    private static class MyCustomObject implements Serializable {
        String key;
        Integer value;
        public MyCustomObject(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        public Integer getValue() {
            return value;
        }

        public String getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyCustomObject that = (MyCustomObject) o;
            return Objects.equals(key, that.key) &&
                    Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "MyCustomObject{" +
                    "key='" + key + '\'' +
                    ", value=" + value +
                    '}';
        }
    }
}
