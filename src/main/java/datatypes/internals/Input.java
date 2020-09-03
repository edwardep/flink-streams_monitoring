package datatypes.internals;

import datatypes.InternalStream;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Objects;

public class Input extends InternalStream {
    private String streamID;
    private long timestamp;
    private Tuple2<Integer, Integer> key;
    private Double val;

    public Input(String streamID, long timestamp, Tuple2<Integer, Integer> key, Double val) {
        this.streamID = streamID;
        this.timestamp = timestamp;
        this.key = key;
        this.val = val;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Tuple2<Integer, Integer> getKey() {
        return key;
    }

    public void setKey(Tuple2<Integer, Integer> key) {
        this.key = key;
    }

    public Double getVal() {
        return val;
    }

    public void setVal(Double val) {
        this.val = val;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Input input = (Input) o;
        return timestamp == input.timestamp &&
                Objects.equals(streamID, input.streamID) &&
                Objects.equals(key, input.key) &&
                Objects.equals(val, input.val);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamID, timestamp, key, val);
    }

    @Override
    public String toString() {
        return "Input{" +
                "streamID='" + streamID + '\'' +
                ", timestamp=" + timestamp +
                ", key=" + key +
                ", val=" + val +
                '}';
    }
}
