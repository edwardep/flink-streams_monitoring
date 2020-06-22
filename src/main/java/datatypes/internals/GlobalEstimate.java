package datatypes.internals;

import datatypes.InternalStream;

public class GlobalEstimate<VectorType> extends InternalStream {
    private String key;
    private VectorType vector;

    public GlobalEstimate(String key, VectorType vector) {
        this.key = key;
        this.vector = vector;
    }

    public String getKey() {
        return key;
    }

    public VectorType getVector() {
        return vector;
    }

    @Override
    public String toString() {
        return "GlobalEstimate{" +
                "key='" + key + '\'' +
                ", vector=" + vector +
                '}';
    }

    @Override
    public String getStreamID() {
        return key;
    }
}
