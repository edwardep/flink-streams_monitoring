package configurations;

import datatypes.InputRecord;
import datatypes.Vector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.*;

import static test_utils.Generators.generateSequence;

public class FgmConfig_test {

    @Test
    public void addVectors_test() {
        FgmConfig cfg = new FgmConfig();

        Vector vec1 = new Vector(generateSequence(5, 10.0));
        Vector vec2 = new Vector(generateSequence(5, 10.0));
        Vector res = cfg.addVectors(vec1, vec2);

        assertEquals(new Vector(generateSequence(5, 20.)), res);
    }

    @Test
    public void subtractVectors_test() {
        FgmConfig cfg = new FgmConfig();

        Vector vec1 = new Vector(generateSequence(10, 5.0));
        Vector vec2 = new Vector(generateSequence(10, 3.0));
        Vector res = cfg.subtractVectors(vec1, vec2);

        assertEquals(new Vector(generateSequence(10, 2.0)), res);
    }

    @Test
    public void scaleVector_test() {
        FgmConfig cfg = new FgmConfig();

        Vector vec = new Vector(generateSequence(10, 5.0));
        Vector res = cfg.scaleVector(vec, 2.0);

        assertEquals(new Vector(generateSequence(10, 10.0)), res);
    }

    @Test
    public void typeInfo_test() {
        FgmConfig cfg = new FgmConfig();
        assertEquals(TypeInformation.of(Vector.class), cfg.getVectorType());
    }

    @Test
    public void batchUpdate_test() {
        FgmConfig cfg = new FgmConfig();

        List<InputRecord> list =  new ArrayList<>();
        list.add(new InputRecord("0", 0L, Tuple2.of(0,0), 1.0));
        list.add(new InputRecord("0", 0L, Tuple2.of(1,1), 1.0));
        list.add(new InputRecord("0", 0L, Tuple2.of(2,2), 1.0));
        list.add(new InputRecord("0", 0L, Tuple2.of(3,3), 1.0));

        Vector res = cfg.batchUpdate(list);

        assertEquals(new Vector(generateSequence(4, 1.0)), res);
    }

    @Test
    public void safeFunction_test() {
        FgmConfig cfg = new FgmConfig();

        Vector drift = new Vector();
        Vector estimate = new Vector(generateSequence(5));

        double phi = cfg.safeFunction(drift, estimate);
    }


}
