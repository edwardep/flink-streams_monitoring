package misc;

import com.esotericsoftware.kryo.util.ObjectMap;
import configurations.AGMSConfig;
import datatypes.InternalStream;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import sources.WorldCupMapSource;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ExitStrategy {

    @Test
    public void exitWithKafkaConsumer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromPropertiesFile("src/test/java/misc/test.properties");
        Properties properties = params.getProperties();

        env.setParallelism(4);
        env
                .addSource(new FlinkKafkaConsumer<>(
                        "input-testset-day46",
                        new SSS(),
                        properties).setStartFromEarliest())
                .setParallelism(1)
                .flatMap(new WorldCupMapSource(new AGMSConfig(params)))
                .returns(TypeInformation.of(InternalStream.class));

        System.out.println(env.getExecutionPlan());
        JobExecutionResult executionResult = env.execute();
        System.out.println("runtime: "+executionResult.getNetRuntime(TimeUnit.MILLISECONDS));
    }


    public static class SSS extends SimpleStringSchema{

        @Override
        public boolean isEndOfStream(String nextElement) {
            return nextElement.equals("EOF");
        }
    }

    @Test
    public void exitWithKafkaProducer() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromPropertiesFile("src/test/java/misc/test.properties");
        Properties properties = params.getProperties();

        FlinkKafkaConsumerBase<String> consumerBase = new FlinkKafkaConsumer<>(
                "test-prod",
                new SimpleStringSchema(),
                properties);

        env.setParallelism(1);
        env
                .fromElements("eins","zwei","drei","vier","funf")
                .addSink(new FlinkKafkaProducer<>(
                        "test-prod",
                        new SimpleStringSchema(),
                        properties)
                );

        env.execute();
    }

    @Test
    public void exitSimpleSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromPropertiesFile("src/test/java/misc/test.properties");
        Properties properties = params.getProperties();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(4);
        env
                .readTextFile("/home/edwardep/Documents/wc_tools/output/test_synth.txt")
//                .addSink(new FlinkKafkaProducer<>(
//                        "test-prod",
//                        new SimpleStringSS(),
//                        properties,
//                        FlinkKafkaProducer.Semantic.NONE)
//                );
                .writeAsText("logs/output-test.txt")
                .setParallelism(1);

        JobExecutionResult executionResult = env.execute();
        System.out.println("runtime: "+executionResult.getNetRuntime(TimeUnit.MILLISECONDS));
    }

    private static class SimpleStringSS implements KafkaSerializationSchema<String> {
        ObjectMapper mapper;
        @Override
        public ProducerRecord<byte[], byte[]> serialize(String s, Long aLong) {
            byte[] value = null;
            if (mapper == null)
                mapper = new ObjectMapper();
            try {
                value = mapper.writeValueAsBytes(s);
            } catch (JsonProcessingException e) {
                System.err.println(e.toString());
            }
            return new ProducerRecord<>("test-prod", value);
        }
    }

}
