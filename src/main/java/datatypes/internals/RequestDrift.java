package datatypes.internals;

import datatypes.InternalStream;

import java.util.Objects;

public class RequestDrift extends InternalStream {
    private String streamID;

    public RequestDrift() { }
    public RequestDrift(String streamID) {
        this.streamID = streamID;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RequestDrift that = (RequestDrift) o;
        return Objects.equals(streamID, that.streamID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamID);
    }

    @Override
    public String toString() {
        return "RequestDrift{" +
                "streamID='" + streamID + '\'' +
                '}';
    }
}
