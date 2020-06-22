package datatypes.internals;

import datatypes.InternalStream;

public class Drift<VectorType> extends InternalStream {
    private long timestamp;
    private VectorType vector;

    public Drift(long timestamp, VectorType vector) {
        this.timestamp = timestamp;
        this.vector = vector;
    }

    public VectorType getVector() {
        return vector;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Drift{" +
                "timestamp=" + timestamp +
                ", vector=" + vector +
                '}';
    }

    @Override
    public String getStreamID() {
        return null;
    }
}
