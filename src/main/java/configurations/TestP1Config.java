package configurations;

import configurations.BaseConfig;
import datatypes.InputRecord;
import datatypes.Vector;
import fgm.SafeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static utils.DoubleOperators.*;
import static utils.DoubleOperators.normalize;

public class TestP1Config implements BaseConfig<Vector, Vector, InputRecord> {
    @Override
    public TypeInformation<Vector> getVectorType() {
        return TypeInformation.of(Vector.class);
    }

    @Override
    public Integer uniqueStreams() {
        return 1;
    }

    @Override
    public Vector newInstance() {
        return new Vector();
    }

    @Override
    public Vector newAccInstance() {
        return new Vector();
    }

    @Override
    public Vector aggregateRecord(InputRecord record, Vector vector) {
        vector.map().put(record.getKey(), vector.getValue(record.getKey()) + record.getVal());
        return vector;
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
    public Vector subtractAccumulators(Vector acc1, Vector acc2) {
        Vector res = new Vector(acc1.map());
        for(Tuple2<Integer, Integer> key : acc2.map().keySet())
            res.map().put(key, res.getValue(key) - acc2.getValue(key));
        return res;
    }

    @Override
    public Vector updateVector(Vector accumulator, Vector vector) {
        for(Map.Entry<Tuple2<Integer, Integer>, Double> entry : accumulator.map().entrySet())
            vector.map().put(entry.getKey(), vector.getValue(entry.getKey()) + entry.getValue());
        return vector;
    }

    @Override
    public double safeFunction(Vector drift, Vector estimate, SafeZone safeZone) {
        double epsilon = 0.20;

        double normEstimate = norm(estimate.map().entrySet());

        // calculate f1(X) = (1+e)|E| - |X+E|
        double f1 = (1.0 + epsilon) * normEstimate - norm(vec_add(estimate.map(), drift.map()).entrySet());

        // calculate f2(X) = X dot (E/|E|) + e|E|
        double f2 = dotProductMap(normalize(estimate.map().entrySet(), normEstimate), drift.map()) + epsilon * normEstimate;

        // select the minimum of the two values
        return Math.min(f1, f2);
    }

    @Override
    public String queryFunction(Vector estimate, long timestamp) {
        double query = 0d;
        for(Double val : estimate.map().values())
            query += val*val;
        return timestamp + "," + query;
    }


    @Override
    public SafeZone initializeSafeZone(Vector global) {
        return null;
    }
}
