package configurations;

import datatypes.InputRecord;
import datatypes.Vector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FgmConfig implements BaseConfig<Vector, InputRecord> {

    @Override
    public TypeInformation<Vector> getVectorType() {
        return TypeInformation.of(Vector.class);
    }

    @Override
    public List<String> getKeyGroup() {
        return new ArrayList<>(Arrays.asList("99", "98", "66", "67", "0", "2", "64", "33", "1", "4",
                "71", "73", "5", "72", "68", "65", "3", "38", "34", "37", "69", "36", "35", "40", "39", "70"));
    }

    @Override
    public Integer getKeyGroupSize() {
        return 26;
    }

    @Override
    public Vector addVectors(Vector vector1, Vector vector2) {
        Vector res = new Vector(vector1.getVector());
        for(Tuple2<Integer, Integer> key : vector2.getVector().keySet())
            res.getVector().put(key, res.getValue(key) + vector2.getValue(key));
        return res;
    }

    @Override
    public Vector scaleVector(Vector vector, Double scalar) {
        return null;
    }

    @Override
    public Vector updateVector(Vector vector, InputRecord record) {
        return null;
    }

    @Override
    public double safeFunction(Vector drift, Vector estimate) {
        return 0;
    }

    @Override
    public String queryFunction(Vector estimate) {
        return null;
    }
}
