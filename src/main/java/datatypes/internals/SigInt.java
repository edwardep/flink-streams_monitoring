package datatypes.internals;

import datatypes.InternalStream;

public class SigInt extends InternalStream {

    private String streamID;

    public SigInt() {
    }

    public SigInt(String streamID) {
        this.streamID = streamID;
    }

    @Override
    public String getStreamID() {
        return streamID;
    }
}
