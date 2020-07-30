package datatypes.internals;

import datatypes.InternalStream;

import java.util.Objects;

public class Quantum extends InternalStream {
    private String streamID;
    private double payload;

    public Quantum() { }

    public Quantum(String streamID, double payload) {
        this.streamID = streamID;
        this.payload = payload;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }

    public double getPayload() {
        return payload;
    }

    public void setPayload(double payload) {
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Quantum quantum = (Quantum) o;
        return Double.compare(quantum.payload, payload) == 0 &&
                Objects.equals(streamID, quantum.streamID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamID, payload);
    }

    @Override
    public String toString() {
        return "Quantum{" +
                "streamID='" + streamID + '\'' +
                ", payload=" + payload +
                '}';
    }
}
