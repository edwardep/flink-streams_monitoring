package datatypes.internals;

import datatypes.InternalStream;

public class RequestZeta  extends InternalStream {
    private String key;
    public RequestZeta(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "RequestZeta{" +
                "key='" + key + '\'' +
                '}';
    }

    @Override
    public String getStreamID() {
        return key;
    }
}
