package datatypes.internals;

import datatypes.InternalStream;

public class InitCoordinator extends InternalStream {
    private int warmup;
    public InitCoordinator(int warmup) {
        this.warmup = warmup;
    }


    public int getWarmup() {
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
