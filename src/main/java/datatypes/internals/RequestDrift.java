package datatypes.internals;

import datatypes.InternalStream;

public class RequestDrift extends InternalStream {
    private String key;
    public RequestDrift(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "RequestDrift{" +
                "key='" + key + '\'' +
                '}';
    }

    @Override
    public String getStreamID() {
        return key;
    }
}
