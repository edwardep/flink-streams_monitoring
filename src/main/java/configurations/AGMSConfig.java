package configurations;

import datatypes.InputRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import sketches.AGMSSketch;
import sketches.SelfJoinAGMS;
import utils.SketchMath;
import utils.SketchOperators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AGMSConfig implements BaseConfig<AGMSSketch, InputRecord> {


    private List<String> keyGroup = new ArrayList<>(Arrays.asList("99", "98", "66", "67", "0", "2", "64", "33", "1", "4",
            "71", "73", "5", "72", "68", "65", "3", "38", "34", "37", "69", "36", "35", "40", "39", "70"));

    @Override
    public TypeInformation<AGMSSketch> getVectorType() {
        return TypeInformation.of(AGMSSketch.class);
    }

    @Override
    public List<String> getKeyGroup() {
        return keyGroup;
    }

    @Override
    public Integer getKeyGroupSize() {
        return keyGroup.size();
    }

    @Override
    public AGMSSketch newInstance() {
        return new AGMSSketch(3,5);
    }

    @Override
    public AGMSSketch addRecord(InputRecord record, AGMSSketch vector) {
        return null;
    }

    @Override
    public AGMSSketch addVectors(AGMSSketch vector1, AGMSSketch vector2) {
        return SketchMath.add(vector1, vector2);
    }

    @Override
    public AGMSSketch subtractVectors(AGMSSketch vector1, AGMSSketch vector2) {
        return SketchMath.subtract(vector1, vector2);
    }

    @Override
    public AGMSSketch scaleVector(AGMSSketch vector, Double scalar) {
        return SketchMath.scale(vector, scalar);
    }

    @Override
    public double safeFunction(AGMSSketch drift, AGMSSketch estimate) {
        //SelfJoinAGMS sz = new SelfJoinAGMS()
        return 0;
    }

    @Override
    public String queryFunction(AGMSSketch estimate, long timestamp) {
        return timestamp+","+SketchOperators.median(estimate.values());
    }

    @Override
    public AGMSSketch batchUpdate(Iterable<InputRecord> iterable) {
        return null;
    }
}
