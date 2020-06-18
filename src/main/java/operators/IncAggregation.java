package operators;

import configurations.BaseConfig;
import datatypes.Vector;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class IncAggregation<VectorType, RecordType> implements AggregateFunction<RecordType, VectorType, VectorType> {

    private BaseConfig<VectorType, RecordType> cfg;
    public IncAggregation(BaseConfig<VectorType, RecordType> cfg) { this.cfg = cfg; }

    @Override
    public VectorType createAccumulator() {
        return cfg.newInstance();
    }

    @Override
    public VectorType add(RecordType inputRecord, VectorType vector) {
        return cfg.addRecord(inputRecord, vector);
    }

    @Override
    public VectorType getResult(VectorType vector) {
        return vector;
    }

    @Override
    public VectorType merge(VectorType vectorType, VectorType acc1) {
        return null;
    }
}