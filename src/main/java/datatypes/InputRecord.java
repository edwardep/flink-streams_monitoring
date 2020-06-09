package datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

public class InputRecord {
    private String streamID;
    private long timestamp;
    private Tuple2<Integer, Integer> key;
    private Double val;

    public InputRecord(String streamID, long timestamp, Tuple2<Integer, Integer> key, Double val){
        this.streamID = streamID;
        this.timestamp = timestamp;
        this.key = key;
        this.val = val;
    }

    public String getStreamID() {
        return streamID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Tuple2<Integer, Integer> getKey() {
        return key;
    }

    public Double getVal() {
        return val;
    }

    @Override
    public String toString() {
        return "InputRecord{" +
                "streamID=" + streamID +
                ", timestamp=" + timestamp +
                ", key=" + key +
                ", val=" + val +
                '}';
    }
}
