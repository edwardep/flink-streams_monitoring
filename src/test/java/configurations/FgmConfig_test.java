package configurations;

import datatypes.Vector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.HashMap;

import static test_utils.Generators.generateSequence;

public class FgmConfig_test {

    @Test
    public void addVectors_test() {
        FgmConfig cfg = new FgmConfig();

        Vector vec1 = new Vector(generateSequence(5));
        Vector vec2 = new Vector(generateSequence(3));

        Vector res = cfg.addVectors(vec1, vec2);

        assert res.getValue(Tuple2.of(0,0)) == 0d;
        assert res.getValue(Tuple2.of(1,1)) == 2d;
        assert res.getValue(Tuple2.of(2,2)) == 4d;
        assert res.getValue(Tuple2.of(3,3)) == 3d;
        assert res.getValue(Tuple2.of(4,4)) == 4d;
    }

    @Test (expected = ArithmeticException.class)
    public void safeFunction_test() {
        FgmConfig cfg = new FgmConfig();

        Vector drift = new Vector();
        Vector estimate = new Vector(generateSequence(5));

        double phi = cfg.safeFunction(drift, estimate);
    }


}
