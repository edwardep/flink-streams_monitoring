package datatypes;
import java.io.Serializable;


/**  */

public class InternalStream<VectorType, RecordType> implements Serializable {
    private long timestamp;
    private String streamID;
    private StreamType type;
    private VectorType vector;
    private RecordType record;
    private Double payload;

    public InternalStream(String streamID, long timestamp, StreamType type, VectorType vector, RecordType record, Double payload)
    {
        this.streamID = streamID;
        this.timestamp = timestamp;
        this.type = type;
        this.vector = vector;
        this.record = record;
        this.payload = payload;
    }

    public static <V, R> InternalStream<V, R> downstreamDrift(long timestamp, V vector) {
        return new InternalStream<>(null, timestamp, StreamType.DRIFT, vector, null, null);
    }

    public static <V, R> InternalStream<V, R> downstreamZeta(Double payload) {
        return new InternalStream<>(null, 0L, StreamType.ZETA, null, null, payload);
    }

    public static <V, R> InternalStream<V, R> downstreamIncrement(Double payload) {
        return new InternalStream<>(null, 0L, StreamType.INCREMENT, null, null, payload);
    }


    public long getTimestamp() { return timestamp; }
    public Integer unionKey() { return 0; }
    public String getStreamID() { return streamID; }
    public StreamType getType() { return type; }
    public VectorType getVector() { return vector; }
    public RecordType getRecord() { return record; }
    public Double getPayload() { return payload; }


    @Override
    public String toString() {
        return "InternalStream{" +
                "streamID=" + streamID +
                ", type=" + type +
                ", vector=" +  vector +
                ", record=" + record +
                ", payload" + payload +
                '}';
    }
}