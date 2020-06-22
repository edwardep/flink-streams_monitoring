package misc;

public class MockDrift<Vec> extends MockInternal {
    private Vec vec;

    public MockDrift(Vec vec) {
        this.vec = vec;
    }

    public Vec getVec() {
        return vec;
    }

    @Override
    public String toString() {
        return "MockDrift{" +
                "vec=" + vec +
                '}';
    }
}
