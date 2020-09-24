package configurations;

import datatypes.InternalStream;
import datatypes.internals.GlobalEstimate;
import datatypes.internals.Input;
import fgm.SafeZone;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.windowing.time.Time;
import sketches.AGMSSketch;
import sketches.SelfJoinAGMS;
import java.util.Map;

import static sketches.SketchMath.*;


public class AGMSConfig implements BaseConfig<AGMSSketch> {

    private int workers = 10;
    private double epsilon = 0.2;
    private boolean window = false;
    private boolean rebalance = false;

    public AGMSConfig() {}

    public AGMSConfig(int workers, double epsilon){
        this.workers = workers;
        this.epsilon = epsilon;
    }
    public AGMSConfig(int workers, double epsilon, boolean window, boolean rebalance){
        this.workers = workers;
        this.epsilon = epsilon;
        this.window = window;
        this.rebalance = rebalance;
    }

    @Override
    public TypeReference<GlobalEstimate<AGMSSketch>> getTypeReference() {
        return new TypeReference<GlobalEstimate<AGMSSketch>>() {};
    }

    @Override
    public boolean slidingWindowEnabled() {
        return window;
    }


    @Override
    public boolean rebalancingEnabled() {
        return rebalance;
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
    public AGMSSketch updateVector(InternalStream inputRecord, AGMSSketch vector) {
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
