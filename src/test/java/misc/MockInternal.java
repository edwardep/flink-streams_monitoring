package misc;

public abstract class MockInternal {

    public static <V> MockInternal sendDrift(V vec) {
        return new MockDrift<>(vec);
    }
}
