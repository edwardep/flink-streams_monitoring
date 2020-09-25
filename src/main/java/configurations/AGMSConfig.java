package configurations;

import datatypes.InternalStream;
import datatypes.internals.GlobalEstimate;
import datatypes.internals.Input;
import fgm.SafeZone;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.windowing.time.Time;
import sketches.AGMSSketch;
import sketches.SelfJoinAGMS;
import java.util.Map;

import static sketches.SketchMath.*;
import static utils.DefJobParameters.*;


public class AGMSConfig extends ExecutionConfig.GlobalJobParameters implements BaseConfig<AGMSSketch> {

    private ParameterTool parameters;

    public AGMSConfig() {}

    public AGMSConfig(ParameterTool parameters){
        this.parameters = parameters;
    }


    @Override
    public TypeReference<GlobalEstimate<AGMSSketch>> getTypeReference() {
        return new TypeReference<GlobalEstimate<AGMSSketch>>() {};
    }

    @Override
    public boolean slidingWindowEnabled() {
        return parameters.getBoolean("sliding-window", false);
    }

    @Override
    public Time windowSize() {
        return Time.seconds(parameters.getInt("window", defWindowSize));
    }

    @Override
    public Time windowSlide() {
        return Time.seconds(parameters.getInt("slide", defSlideSize));
    }

    @Override
    public boolean rebalancingEnabled() {
        return parameters.getBoolean("rebalance", false);
    }

    @Override
    public TypeInformation<AGMSSketch> getVectorType() {
        return TypeInformation.of(AGMSSketch.class);
    }

    @Override
    public Integer workers() {
        return parameters.getInt("workers", defWorkers);
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
        double e = parameters.getDouble("epsilon", defEpsilon);
        double med = median(E.values());
        return new SelfJoinAGMS(E.values(), (1-e)*med, (1+e)*med, true);
    }
}
