package datatypes.internals;

import datatypes.InternalStream;

public class Increment extends InternalStream {
    private int payload;

    public Increment() {}

    public Increment(int payload){
        this.payload = payload;
    }

    public int getPayload() {
        return payload;
    }

    public void setPayload(int payload) {
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Increment{" +
                "payload=" + payload +
                '}';
    }

    @Override
    public String getStreamID() {
        return null;
    }
}
