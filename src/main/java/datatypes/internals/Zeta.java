package datatypes.internals;

import datatypes.InternalStream;

public class Zeta extends InternalStream {
    private double payload;

    public Zeta() {
    }

    public Zeta(double payload) {
        this.payload = payload;
    }

    public double getPayload() {
        return payload;
    }

    public void setPayload(double payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Zeta{" +
                "payload=" + payload +
                '}';
    }

    @Override
    public String getStreamID() {
        return null;
    }
}
