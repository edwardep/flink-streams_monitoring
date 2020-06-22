package datatypes.internals;

import datatypes.InternalStream;

public class Lambda extends InternalStream {
    private String key;
    private double lambda;
    public Lambda(String key, double lambda) {
        this.key = key;
        this.lambda = lambda;
    }

    public String getKey() {
        return key;
    }

    public double getLambda() {
        return lambda;
    }

    @Override
    public String toString() {
        return "Lambda{" +
                "key='" + key + '\'' +
                ", lambda=" + lambda +
                '}';
    }

    @Override
    public String getStreamID() {
        return key;
    }
}
