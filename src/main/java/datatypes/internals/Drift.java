package datatypes.internals;

import datatypes.InternalStream;

public class Drift<VectorType> extends InternalStream {
    private long timestamp;
    private VectorType vector;

    public Drift() {}
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

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setVector(VectorType vector) {
        this.vector = vector;
    }

    @Override
    public String getStreamID() {
        return null;
    }
}
