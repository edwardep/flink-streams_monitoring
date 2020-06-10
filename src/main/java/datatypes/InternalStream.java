package datatypes;
import java.io.Serializable;


/**  */

public class InternalStream<VectorType> implements Serializable {
    private long timestamp;
    private String streamID;
    private StreamType type;
    private VectorType vector;
    private Double payload;

    public InternalStream(String streamID, long timestamp, StreamType type, VectorType vector, Double payload)
    {
        this.streamID = streamID;
        this.timestamp = timestamp;
        this.type = type;
        this.vector = vector;
        this.payload = payload;
    }

    public static <V> InternalStream downstreamDrift(long timestamp, V vector) {
        return new InternalStream<>(null, timestamp, StreamType.DRIFT, vector, null);
    }
    public static InternalStream downstreamZeta(Double payload) {
        return new InternalStream<>(null, 0L, StreamType.ZETA, null, payload);
    }
    public static InternalStream downstreamIncrement(Double payload) {
        return new InternalStream<>(null, 0L, StreamType.INCREMENT, null, payload);
    }
    public static <V> InternalStream upstreamGlobalEstimate(String key, V vector) {
        return new InternalStream<>(key, 0L, StreamType.HYPERPARAMETERS, vector,null);
    }
    public static InternalStream upstreamQuantum(String key, Double payload) {
        return new InternalStream<>(key, 0L, StreamType.QUANTUM, null, payload);
    }
    public static InternalStream upstreamLambda(String key, Double payload) {
        return new InternalStream<>(key, 0L, StreamType.BALANCE, null, payload);
    }
    public static InternalStream upstreamRequestDrift(String key) {
        return new InternalStream<>(key, 0L, StreamType.REQ_DRIFT, null, null);
    }
    public static InternalStream upstreamRequestZeta(String key) {
        return new InternalStream<>(key, 0L, StreamType.REQ_ZETA, null,null);
    }
    public static <V> InternalStream slideAggregate(String key, V vector) {
        return new InternalStream<>(key, 0L, StreamType.INPUT, vector, null);
    }
    public static <V> InternalStream windowSlide(String key, long timestamp, V vector) {
        return new InternalStream<>(key, timestamp, StreamType.INPUT, vector,null);
    }
    public static <V> InternalStream initializeCoordinator(long warmup, V vector) {
        return new InternalStream<>("0", warmup, StreamType.INIT, vector, null);
    }
    public static InternalStream emptyStream() {
        return new InternalStream<>("0", 0L, StreamType.INIT, null,null);
    }


    public long getTimestamp() { return timestamp; }
    public String unionKey() { return "0"; }
    public String getStreamID() { return streamID; }
    public StreamType getType() { return type; }
    public VectorType getVector() { return vector; }
    public Double getPayload() { return payload; }


    @Override
    public String toString() {
        return "InternalStream{" +
                "streamID=" + streamID +
                ", timestamp=" + timestamp +
                ", type=" + type +
                ", vector=" +  vector +
                ", payload=" + payload +
                '}';
    }
}
