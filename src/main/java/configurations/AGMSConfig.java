package configurations;

import datatypes.InputRecord;
import datatypes.Vector;
import fgm.SafeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import sketches.AGMSSketch;
import sketches.SelfJoinAGMS;

import java.util.Map;

import static sketches.SketchMath.*;


public class AGMSConfig implements BaseConfig<Vector, AGMSSketch, InputRecord> {

    @Override
    public TypeInformation<AGMSSketch> getVectorType() {
        return TypeInformation.of(AGMSSketch.class);
    }

    @Override
    public Integer uniqueStreams() {
        return 10;
    }

    @Override
    public AGMSSketch newInstance() {
        return new AGMSSketch(7,5000);
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
    public Vector subtractAccumulators(Vector acc1, Vector acc2) {
        Vector res = new Vector(acc1.map());
        for(Tuple2<Integer,Integer> key : acc2.map().keySet())
            res.map().put(key, res.getValue(key) - acc2.getValue(key));
        return res;
    }

    @Override
    public AGMSSketch updateVector(Vector accumulator, AGMSSketch vector) {
        for (Map.Entry<Tuple2<Integer,Integer>, Double> entry : accumulator.map().entrySet()) {
            long key = entry.getKey().hashCode(); // you could hash the 2 fields separately and concat them into a long
            vector.update(key, entry.getValue());
        }
        return vector;
    }

    @Override
    public AGMSSketch addVectors(AGMSSketch vector1, AGMSSketch vector2) {
        return add(vector1, vector2);
    }

    @Override
    public AGMSSketch scaleVector(AGMSSketch vector, Double scalar) {
        return scale(vector, scalar);
    }

    @Override
    public double safeFunction(AGMSSketch drift, AGMSSketch estimate, SafeZone safeZone) {
        AGMSSketch XE = add(drift, estimate);
        return ((SelfJoinAGMS) safeZone).zeta(XE.values());
    }

    @Override
    public String queryFunction(AGMSSketch estimate, long timestamp) {
        return timestamp+","+median(estimate.values());
    }

    @Override
    public SafeZone initializeSafeZone(AGMSSketch E) {
        double e = 0.1;
        double med = median(E.values());
        return new SelfJoinAGMS(E.values(), (1-e)*med, (1+e)*med, false);
    }
}
