package operators;

import configurations.BaseConfig;
import datatypes.Accumulator;
import org.apache.flink.api.common.functions.AggregateFunction;

public class IncAggregation<AccType, RecordType> implements AggregateFunction<RecordType, Accumulator<AccType>, Accumulator<AccType>> {

    private BaseConfig<AccType, ?, RecordType> cfg;
    public IncAggregation(BaseConfig<AccType, ?, RecordType> cfg) { this.cfg = cfg; }


    @Override
    public Accumulator<AccType> createAccumulator() {
        return new Accumulator<>(cfg.newAccInstance());
    }

    @Override
    public Accumulator<AccType> add(RecordType input, Accumulator<AccType> accumulator) {
        accumulator.setVec(cfg.aggregateRecord(input, accumulator.getVec()));
        return accumulator;
    }

    @Override
    public Accumulator<AccType> getResult(Accumulator<AccType> accumulator) {
        return accumulator;
    }

    @Override
    public Accumulator<AccType> merge(Accumulator<AccType> acc1, Accumulator<AccType> acc2) {
        return null;
    }
}