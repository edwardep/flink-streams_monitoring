package datatypes.internals;

import datatypes.InternalStream;

public class Quantum extends InternalStream {
    private String key;
    private double payload;

    public Quantum(String key, double payload){
        this.key = key;
        this.payload = payload;
    }

    public String getKey() {
        return key;
    }

    public double getPayload() {
        return payload;
    }

    @Override
    public String toString() {
        return "Quantum{" +
                "key='" + key + '\'' +
                ", payload=" + payload +
                '}';
    }
    @Override
    public String getStreamID() {
        return key;
    }
}
