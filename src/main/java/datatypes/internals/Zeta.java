package datatypes.internals;

import datatypes.InternalStream;

public class Zeta extends InternalStream {
    private double payload;

    public Zeta(double payload) {
        this.payload = payload;
    }

    public double getPayload() {
        return payload;
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
