package datatypes;

import java.io.Serializable;
import java.util.Objects;

public class Accumulator<VectorType> implements Serializable {

    private String streamID;
    private VectorType vec;

    public Accumulator(VectorType vec) {
        this.vec = vec;
    }
    public Accumulator(String streamID, VectorType vec) {
        this.streamID = streamID;
        this.vec = vec;
    }

    public String getStreamID() { return streamID; }

    public VectorType getVec() {
        return vec;
    }

    public void setVec(VectorType vec) {
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
