package datatypes.internals;

import datatypes.InternalStream;

public class Heartbeat extends InternalStream {
    private String streamID;

    public Heartbeat() {
    }

    public Heartbeat(String streamID) {
        this.streamID = streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }

    @Override
    public String toString() {
        return "Heartbeat{" +
                "streamID='" + streamID + '\'' +
                '}';
    }
}
