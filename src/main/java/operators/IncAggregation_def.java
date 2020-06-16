package operators;

import configurations.BaseConfig;
import datatypes.InputRecord;
import datatypes.Vector;
import org.apache.flink.api.common.functions.AggregateFunction;

public class IncAggregation_def implements AggregateFunction<InputRecord, Vector, Vector> {

    private BaseConfig<Vector, InputRecord> cfg;
    public IncAggregation_def(BaseConfig<Vector, InputRecord> cfg) { this.cfg = cfg; }

    @Override
    public Vector createAccumulator() {
        return cfg.newInstance();
    }

    @Override
    public Vector add(InputRecord inputRecord, Vector vector) {
        return cfg.addRecord(inputRecord, vector);
    }

    @Override
    public Vector getResult(Vector vector) {
        return vector;
    }

    @Override
    public Vector merge(Vector vectorType, Vector acc1) {
        return null;
    }
}