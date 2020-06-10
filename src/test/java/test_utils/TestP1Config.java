package test_utils;

import configurations.BaseConfig;
import datatypes.InputRecord;
import datatypes.Vector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static utils.DoubleOperators.*;
import static utils.DoubleOperators.normalize;

public class TestP1Config implements BaseConfig<Vector, InputRecord> {
    @Override
    public TypeInformation<Vector> getVectorType() {
        return TypeInformation.of(Vector.class);
    }

    @Override
    public List<String> getKeyGroup() {
        return new ArrayList<>(Collections.singletonList("1"));
    }

    @Override
    public Integer getKeyGroupSize() {
        return 1;
    }

    @Override
    public Vector newInstance() {
        return new Vector();
    }

    @Override
    public Vector addVectors(Vector vector1, Vector vector2) {
        Vector res = new Vector(vector1.map());
        for(Tuple2<Integer, Integer> key : vector2.map().keySet())
            res.map().put(key, res.getValue(key) + vector2.getValue(key));
        return res;
    }

    @Override
    public Vector scaleVector(Vector vector, Double scalar) {
        Vector res = new Vector();
        for(Tuple2<Integer, Integer> key : vector.map().keySet())
            res.map().put(key, vector.getValue(key) * scalar);
        return res;
    }

    @Override
    public Vector subtractVectors(Vector vector1, Vector vector2) {
        Vector res = new Vector(vector1.map());
        for(Tuple2<Integer, Integer> key : vector2.map().keySet())
            res.map().put(key, res.getValue(key) - vector2.getValue(key));
        return res;
    }

    @Override
    public double safeFunction(Vector drift, Vector estimate) {
        double epsilon = 0.20;

        double normEstimate = norm(estimate.map().entrySet());

        if(drift == null)
            return - epsilon * normEstimate;

        // calculate f1(X) = |X+E| -(1+e)|E|
        double f1 = norm(vec_add(estimate.map(), drift.map()).entrySet()) - (1.0 + epsilon) * normEstimate;

        // calculate f2(X) = -e|E| - X dot (E/|E|)
        double f2 = -epsilon * normEstimate - dotProductMap(normalize(estimate.map().entrySet(), normEstimate), drift.map());

        // select the maximum of the two values
        return Math.max(f1, f2);
    }

    @Override
    public String queryFunction(Vector estimate, long timestamp) {
        double query = 0d;
        for(Double val : estimate.map().values())
            query += val*val;
        return timestamp + "," + query;
    }

    @Override
    public Vector batchUpdate(Iterable<InputRecord> iterable) {
        Vector res = new Vector();
        for(InputRecord record : iterable)
            res.map().put(record.getKey(), record.getVal());
        return res;
    }
}