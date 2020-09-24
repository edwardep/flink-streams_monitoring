package configurations;


import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.GlobalEstimate;
import datatypes.internals.Input;
import fgm.SafeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import static utils.DoubleOperators.*;

public class TestP4Config implements BaseConfig<Vector> {

    @Override
    public TypeReference<GlobalEstimate<Vector>> getTypeReference() {
        return null;
    }

    @Override
    public TypeInformation<Vector> getVectorType() {
        return TypeInformation.of(Vector.class);
    }

    @Override
    public Integer workers() {
        return 4;
    }

    @Override
    public Vector newVectorInstance() {
        return new Vector();
    }

    @Override
    public Vector addVectors(Vector vector1, Vector vector2) {
        Vector res = new Vector(vector1);
        for (Tuple2<Integer, Integer> key : vector2.keySet())
            res.put(key, res.getOrDefault(key, 0d) + vector2.get(key));
        return res;
    }

    @Override
    public Vector scaleVector(Vector vector, Double scalar) {
        Vector res = new Vector();
        for (Tuple2<Integer, Integer> key : vector.keySet())
            res.put(key, vector.get(key) * scalar);
        return res;
    }

    @Override
    public Vector updateVector(InternalStream inputRecord, Vector vector) {
        Tuple2<Integer, Integer> key = ((Input)inputRecord).getKey();
        Double val = ((Input)inputRecord).getVal();
        vector.put(key, vector.getOrDefault(key, 0d) + val);
        return vector;
    }


    @Override
    public double safeFunction(Vector drift, Vector estimate, SafeZone safeZone) {
        double epsilon = 0.2;

        double normEstimate = norm(estimate.entrySet());

        // calculate f1(X) = -|X+E| +(1+e)|E|
        double f1 = -norm(vec_add(estimate, drift).entrySet()) + (1.0 + epsilon) * normEstimate;

        // calculate f2(X) = e|E| + X dot (E/|E|)
        double f2 = epsilon * normEstimate + dotProductMap(normalize(estimate.entrySet(), normEstimate), drift);

        // select the maximum of the two values
        return Math.min(f1, f2);
    }

    @Override
    public String queryFunction(Vector estimate, long timestamp) {
        double query = 0d;
        for (Double val : estimate.values())
            query += val * val;
        return timestamp + "," + query;
    }


    @Override
    public SafeZone initializeSafeZone(Vector global) {
        return null;
    }
}
