package kafka;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.internals.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

import static jobs.MonitoringJobWithKafka.endOfFile;
import static utils.DefJobParameters.*;

public class KafkaUtils {

    /**
     * Consumes data from a Kafka topic and returns a string for each line of the data file.
     * @param parameters The arguments of the program.
     * @return A Kafka Consumer or type String which can be passed to the .addSource() operator
     */
    public static FlinkKafkaConsumer<String> createConsumerInput(ParameterTool parameters) {
        String topic = parameters.get("input-topic", defInputTopic);
        return new FlinkKafkaConsumer<>(
                topic,
                new FiniteSimpleStringSchema(),
                createProperties(parameters, "input-group"));
    }

    /**
     * Writes feedback data to a kafka topic. This is the beginning of the feedback loop.
     * @param parameters The arguments of the program.
     * @return A Kafka Producer which can be passed to the .addSink() operator
     */
    public static FlinkKafkaProducer<InternalStream> createProducerInternal(ParameterTool parameters) {
        String topic = parameters.get("feedback-topic", defFeedbackTopic);
        return new FlinkKafkaProducer<>(
                topic,
                new KafkaSerializationSchema<InternalStream>() {
                    ObjectMapper mapper;

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(InternalStream record, @Nullable Long aLong) {
                        byte[] type = null;
                        byte[] value = null;
                        if (mapper == null)
                            mapper = new ObjectMapper();
                        try {
                            type = mapper.writeValueAsBytes(record.type);
                            value = mapper.writeValueAsBytes(record);
                        } catch (JsonProcessingException e) {
                            System.err.println(e.toString());
                        }

                        return new ProducerRecord<>(topic, Integer.parseInt(record.getStreamID()), type, value);
                    }
                },
                createProperties(parameters),
                FlinkKafkaProducer.Semantic.NONE);
    }

    /**
     * Consumes feedback data from a topic and feeds it into the WorkerProcess. This acts as the tail of the feedback
     * loop.
     * @param parameters The arguments of the program.
     * @param cfg   The BaseConfig for accessing user-defines types
     * @return A Kafka Consumer which can be passed to the .addSource() operator
     */
    public static FlinkKafkaConsumer<InternalStream> createConsumerInternal(ParameterTool parameters, BaseConfig<?, ?, ?> cfg) {
        Random rand = new Random();
        String topic = parameters.get("feedback-topic", defFeedbackTopic);
        return new FlinkKafkaConsumer<>(
                topic,
                new KafkaDeserializationSchema<InternalStream>() {
                    //boolean sigInt = false;
                    ObjectMapper mapper;
                    @Override
                    public boolean isEndOfStream(InternalStream internalStream) {
                        return internalStream.type.equals("SigInt");
                    }

                    @Override
                    public InternalStream deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        InternalStream retVal;
                        String type = null;
                        if (mapper == null)
                            mapper = new ObjectMapper();
                        if (record.key() != null)
                            type = mapper.readValue(record.key(), String.class);

                        try {
                            switch (Objects.requireNonNull(type)) {
                                case "Quantum":
                                    retVal = mapper.readValue(record.value(), Quantum.class);
                                    break;
                                case "GlobalEstimate":
                                    retVal = mapper.readValue(record.value(), cfg.getTypeReference());
                                    break;
                                case "Lambda":
                                    retVal = mapper.readValue(record.value(), Lambda.class);
                                    break;
                                case "RequestZeta":
                                    retVal = mapper.readValue(record.value(), RequestZeta.class);
                                    break;
                                case "RequestDrift":
                                    retVal = mapper.readValue(record.value(), RequestDrift.class);
                                    break;
                                case "SigInt":
                                    retVal = mapper.readValue(record.value(), SigInt.class);
                                    break;
                                default:
                                    throw new UnsupportedOperationException("This object type is not supported by the kafka deserialization schema.");
                            }
                        } catch (Exception ex) {
                            System.out.println(ex.toString());
                            retVal = null;
                        }
                        return retVal;
                    }

                    @Override
                    public TypeInformation<InternalStream> getProducedType() {
                        return TypeInformation.of(InternalStream.class);
                    }
                },
                createProperties(parameters, "iter-group-" + rand.nextLong()));
    }


    /**
     * Utility method for creating properties object for a Kafka Consumer.
     * @param parameters The arguments of the program
     * @param groupId   The consumer group.id
     * @return An initialized Properties object
     */
    private static Properties createProperties(ParameterTool parameters, String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameters.get("kafka-servers", defKafkaServers));
        properties.setProperty("group.id", groupId);
        return properties;
    }

    /**
     * Utility method for creating properties object for a Kafka Producer.
     * @param parameters The arguments of the program
     * @return An initialized Properties object
     */
    private static Properties createProperties(ParameterTool parameters) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameters.get("kafka-servers", defKafkaServers));
        return properties;
    }

    private static class FiniteSimpleStringSchema extends SimpleStringSchema {
        @Override
        public boolean isEndOfStream(String nextElement) {
            if(nextElement.equals("EOF")){
                endOfFile = true;
                return true;
            }
            else {
                return false;
            }
        }
    }
}
