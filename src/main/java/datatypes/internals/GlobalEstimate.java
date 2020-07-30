package datatypes.internals;

import datatypes.InternalStream;

import java.util.Objects;

public class GlobalEstimate<VectorType> extends InternalStream {
    private String streamID;
    private VectorType vector;

    public GlobalEstimate() {
    }

    public GlobalEstimate(String streamID, VectorType vector) {
        this.streamID = streamID;
        this.vector = vector;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    public VectorType getVector() {
        return vector;
    }

    public void setVector(VectorType vector) {
        this.vector = vector;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GlobalEstimate<?> that = (GlobalEstimate<?>) o;
        return Objects.equals(streamID, that.streamID) &&
                Objects.equals(vector, that.vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamID, vector);
    }

    @Override
    public String toString() {
        return "GlobalEstimate{" +
                "streamID='" + streamID + '\'' +
                ", vector=" + vector +
                '}';
    }
}
