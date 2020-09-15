package configurations;

import datatypes.InternalStream;
import datatypes.internals.GlobalEstimate;
import datatypes.internals.Input;
import fgm.SafeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import sketches.AGMSSketch;
import sketches.SelfJoinAGMS;

import java.util.HashMap;
import java.util.Map;

import static sketches.SketchMath.*;


public class AGMSConfig implements BaseConfig<Map<Tuple2<Integer, Integer>, Double>, AGMSSketch, InternalStream> {

    private int workers = 10;
    private double epsilon = 0.2;

    public AGMSConfig() {}

    public AGMSConfig(int workers, double epsilon){
        this.workers = workers;
        this.epsilon = epsilon;
    }

    @Override
    public TypeInformation<Map<Tuple2<Integer, Integer>, Double>> getAccType() {
        return Types.MAP(Types.TUPLE(Types.INT, Types.INT), Types.DOUBLE);
    }

    @Override
    public TypeReference<GlobalEstimate<AGMSSketch>> getTypeReference() {
        return new TypeReference<GlobalEstimate<AGMSSketch>>() {};
    }

    @Override
    public TypeInformation<AGMSSketch> getVectorType() {
        return TypeInformation.of(AGMSSketch.class);
    }

    @Override
    public Integer workers() {
        return workers;
    }

    @Override
    public AGMSSketch newVectorInstance() {
        return new AGMSSketch(7,3000);
    }

    @Override
    public Map<Tuple2<Integer, Integer>, Double> newAccInstance() { return new HashMap<>(); }

    @Override
    public Map<Tuple2<Integer, Integer>, Double> aggregateRecord(InternalStream record, Map<Tuple2<Integer, Integer>, Double> vector) {
        Tuple2<Integer, Integer> key = ((Input)record).getKey();
        Double val = ((Input)record).getVal();
        vector.put(key, vector.getOrDefault(key, 0d) + val);
        return vector;
    }

    @Override
    public Map<Tuple2<Integer, Integer>, Double> subtractAccumulators(Map<Tuple2<Integer, Integer>, Double> acc1, Map<Tuple2<Integer, Integer>, Double> acc2) {
        Map<Tuple2<Integer, Integer>, Double> res = new HashMap<>(acc1);
        for(Tuple2<Integer,Integer> key : acc2.keySet())
            res.put(key, res.getOrDefault(key, 0d) - acc2.get(key));
        return res;
    }

    @Override
    public AGMSSketch updateVector(Map<Tuple2<Integer, Integer>, Double> accumulator, AGMSSketch vector) {
        for (Map.Entry<Tuple2<Integer,Integer>, Double> entry : accumulator.entrySet()) {
            long key = entry.getKey().hashCode(); // you could hash the 2 fields separately and concat them into a long
            vector.update(key, entry.getValue());
        }
        return vector;
    }

    @Override
    public AGMSSketch updateVectorCashRegister(InternalStream inputRecord, AGMSSketch vector) {
        long key = ((Input)inputRecord).getKey().hashCode(); // you could hash the 2 fields separately and concat them into a long
        vector.update(key, ((Input)inputRecord).getVal());
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
        double e = epsilon;
        double med = median(E.values());
        return new SelfJoinAGMS(E.values(), (1-e)*med, (1+e)*med, true);
    }
}
