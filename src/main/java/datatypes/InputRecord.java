package datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

public class InputRecord {
    private String streamID;
    private long timestamp;
    private Tuple2<Tuple2<Integer, Integer>, Double> tuple;

    public InputRecord(String streamID, long timestamp, Tuple2<Tuple2<Integer, Integer>, Double> tuple){
        this.streamID = streamID;
        this.timestamp = timestamp;
        this.tuple = tuple;
    }

    public String getStreamID() {
        return streamID;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Tuple2<Tuple2<Integer, Integer>, Double> getTuple() {
        return tuple;
    }

    @Override
    public String toString() {
        return "InputRecord{" +
                "streamID=" + streamID +
                ", timestamp=" + timestamp +
                ", tuple=" + tuple +
                '}';
    }
}
