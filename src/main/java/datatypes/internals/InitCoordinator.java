package datatypes.internals;

import datatypes.InternalStream;

public class InitCoordinator extends InternalStream {
    private long warmup;
    public InitCoordinator(long warmup) {
        this.warmup = warmup;
    }


    public long getWarmup() {
        return warmup;
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
