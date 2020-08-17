package configurations;

import datatypes.InputRecord;
import datatypes.Vector;
import fgm.SafeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static utils.DoubleOperators.*;

@Deprecated
public class FgmConfig implements BaseConfig<Vector, Vector, InputRecord> {

    @Override
    public TypeInformation<Vector> getAccType() {
        return null;
    }

    @Override
    public TypeInformation<Vector> getVectorType() {
        return TypeInformation.of(Vector.class);
    }

    @Override
    public Integer workers() {
        return 10;
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
        for(Tuple2<Integer,Integer> key : vector.map().keySet())
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

    //BATCH UPDATE not implemented
    @Override
    public Vector updateVector(Vector accumulator, Vector vector) {
        return null;
    }

    @Override
    public double safeFunction(Vector drift, Vector estimate, SafeZone safeZone) {
        double epsilon = 0.2;

        double normEstimate = norm(estimate.map().entrySet());

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
        return (timestamp+1) + "," + query;
    }

    @Override
    public SafeZone initializeSafeZone(Vector global) {
        return null;
    }
}
