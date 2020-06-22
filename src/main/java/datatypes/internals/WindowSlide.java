package datatypes.internals;

import datatypes.InternalStream;

public class WindowSlide<VectorType> extends InternalStream {
    private String key;
    private long timestamp;
    private VectorType vector;

    public WindowSlide(String key, long timestamp, VectorType vector) {
        this.key = key;
        this.timestamp = timestamp;
        this.vector = vector;
    }

    public String getKey() {
        return key;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public VectorType getVector() {
        return vector;
    }

    @Override
    public String toString() {
        return "WindowSlide{" +
                "key='" + key + '\'' +
                ", timestamp=" + timestamp +
                ", vector=" + vector +
                '}';
    }

    @Override
    public String getStreamID() {
        return key;
    }
}
