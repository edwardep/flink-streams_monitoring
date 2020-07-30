package datatypes.internals;

import datatypes.InternalStream;

import java.util.Objects;

public class Lambda extends InternalStream {
    private String streamID;
    private double lambda;

    public Lambda() { }

    public Lambda(String streamID, double lambda) {
        this.streamID = streamID;
        this.lambda = lambda;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }

    public void setStreamID(String streamID) {
        this.streamID = streamID;
    }
    public double getLambda() {
        return lambda;
    }

    public void setLambda(double lambda) {
        this.lambda = lambda;
    }

    @Override
    public String toString() {
        return "Lambda{" +
                "streamID='" + streamID + '\'' +
                ", lambda=" + lambda +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lambda lambda1 = (Lambda) o;
        return Double.compare(lambda1.lambda, lambda) == 0 &&
                Objects.equals(streamID, lambda1.streamID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(streamID, lambda);
    }
}
