package datatypes.internals;

import datatypes.InternalStream;

import java.util.Objects;

public class WindowSlide<Acc> extends InternalStream {
    private String streamID;
    private Acc vector;

    public WindowSlide() {
    }

    public WindowSlide(String streamID, Acc vector) {
        this.streamID = streamID;
        this.vector = vector;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    public Acc getVector() {
        return vector;
    }

    public void setVector(Acc vector) {
        this.vector = vector;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowSlide<?> that = (WindowSlide<?>) o;
        return Objects.equals(streamID, that.streamID) &&
                Objects.equals(vector, that.vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamID, vector);
    }

    @Override
    public String toString() {
        return "WindowSlide{" +
                "streamID='" + streamID + '\'' +
                ", vector=" + vector +
                '}';
    }
}
