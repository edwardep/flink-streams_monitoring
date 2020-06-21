package datatypes;

import java.io.Serializable;
import java.util.Objects;

public class Accumulator<AccType> implements Serializable {

    private String streamID;
    private AccType vec;

    public Accumulator(AccType vec) {
        this.vec = vec;
    }
    public Accumulator(String streamID, AccType vec) {
        this.streamID = streamID;
        this.vec = vec;
    }

    public String getStreamID() { return streamID; }

    public AccType getVec() {
        return vec;
    }

    public void setVec(AccType vec) {
        this.vec = vec;
    }

    @Override
    public String toString() {
        return vec.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Accumulator<?> that = (Accumulator<?>) o;
        return Objects.equals(vec, that.vec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vec);
    }
}
