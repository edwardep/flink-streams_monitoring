package datatypes.internals;

import datatypes.InternalStream;

public class FirstFlush extends InternalStream {
    private String key;
    private int warmup;

    public FirstFlush(String key, int warmup) {
        this.key = key;
        this.warmup = warmup;
    }

    public String getKey() {
        return key;
    }

    public int getWarmup() {
        return warmup;
    }

    @Override
    public String getStreamID() {
        return key;
    }

    @Override
    public String toString() {
        return "FirstFlush{" +
                "key='" + key + '\'' +
                ", warmup=" + warmup +
                '}';
    }
}
