package configurations;


import configurations.BaseConfig;
import datatypes.InputRecord;
import datatypes.Vector;
import fgm.SafeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static utils.DoubleOperators.*;

public class TestP4Config implements BaseConfig<Vector, InputRecord> {

    private double epsilon = 0.20;

    public TestP4Config() {}
    public TestP4Config(double epsilon) { this.epsilon = epsilon; }

    @Override
    public TypeInformation<Vector> getVectorType() {
        return TypeInformation.of(Vector.class);
    }

    @Override
    public List<String> getKeyGroup() {
        return new ArrayList<>(Arrays.asList("0","1","2","3"));
    }

    @Override
    public Integer getKeyGroupSize() {
        return 4;
    }

    @Override
    public Vector newInstance() {
        return new Vector();
    }

    @Override
    public Vector addRecord(InputRecord record, Vector vector) {
        vector.map().put(record.getKey(), vector.getValue(record.getKey()) + record.getVal());
        return vector;
    }

    @Override
    public Vector addVectors(Vector vector1, Vector vector2) {
        Vector res = new Vector(vector1.map());
        for (Tuple2<Integer, Integer> key : vector2.map().keySet())
            res.map().put(key, res.getValue(key) + vector2.getValue(key));
        return res;
    }

    @Override
    public Vector scaleVector(Vector vector, Double scalar) {
        Vector res = new Vector();
        for (Tuple2<Integer, Integer> key : vector.map().keySet())
            res.map().put(key, vector.getValue(key) * scalar);
        return res;
    }

    @Override
    public Vector subtractVectors(Vector vector1, Vector vector2) {
        Vector res = new Vector(vector1.map());
        for (Tuple2<Integer, Integer> key : vector2.map().keySet())
            res.map().put(key, res.getValue(key) - vector2.getValue(key));
        return res;
    }

    @Override
    public double safeFunction(Vector drift, Vector estimate) {

        double normEstimate = norm(estimate.map().entrySet());

        if (drift == null)
            return -epsilon * normEstimate;

        // calculate f1(X) = |X+E| -(1+e)|E|
        double f1 = norm(vec_add(estimate.map(), drift.map()).entrySet()) - (1.0 + epsilon) * normEstimate;

        // calculate f2(X) = -e|E| - X dot (E/|E|)
        double f2 = -epsilon * normEstimate - dotProductMap(normalize(estimate.map().entrySet(), normEstimate), drift.map());

        // select the maximum of the two values
        return Math.max(f1, f2);
    }

    @Override
    public double safeFunction(Vector drift, Vector estimate, SafeZone safeZone) {
        return 0;
    }

    @Override
    public String queryFunction(Vector estimate, long timestamp) {
        double query = 0d;
        for (Double val : estimate.map().values())
            query += val * val;
        return timestamp + "," + query;
    }

    @Override
    public Vector batchUpdate(Iterable<InputRecord> iterable) {
        Vector res = new Vector();
        for (InputRecord record : iterable)
            res.map().put(record.getKey(), res.map().getOrDefault(record.getKey(), 0d) + record.getVal());
        return res;
    }

    @Override
    public SafeZone initializeSafeZone(Vector global) {
        return null;
    }
}
