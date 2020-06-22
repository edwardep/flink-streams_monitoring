package datatypes.internals;

import datatypes.InternalStream;

public class Increment extends InternalStream {
    private int payload;
    public Increment(int payload){
        this.payload = payload;
    }

    public int getPayload() {
        return payload;
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
