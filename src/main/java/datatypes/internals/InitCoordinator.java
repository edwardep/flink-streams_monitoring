package datatypes.internals;

import datatypes.InternalStream;

public class InitCoordinator extends InternalStream {
    private long warmup;

    public InitCoordinator() {
    }

    public InitCoordinator(long warmup) {
        this.warmup = warmup;
    }


    public long getWarmup() {
        return warmup;
    }

    public void setWarmup(long warmup) {
        this.warmup = warmup;
    }

    @Override
    public String toString() {
        return "InitCoordinator{" +
                "warmup=" + warmup +
                '}';
    }

    @Override
    public String getStreamID() {
        return null;
    }
}
