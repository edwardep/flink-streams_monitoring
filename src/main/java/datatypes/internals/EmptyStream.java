package datatypes.internals;

import datatypes.InternalStream;

public class EmptyStream extends InternalStream {
    public EmptyStream() { }

    @Override
    public String getStreamID() {
        return null;
    }
}
