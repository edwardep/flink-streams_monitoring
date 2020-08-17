package operators;

import configurations.BaseConfig;
import org.apache.flink.api.common.functions.AggregateFunction;

public class IncAggregation<In, Acc> implements AggregateFunction<In, Acc, Acc> {

    private BaseConfig<Acc, ?, In> cfg;
    public IncAggregation(BaseConfig<Acc, ?, In> cfg) { this.cfg = cfg; }

    @Override
    public Acc createAccumulator() {
        return cfg.newAccInstance();
    }

    @Override
    public Acc add(In in, Acc acc) {
        return cfg.aggregateRecord(in, acc);
    }

    @Override
    public Acc getResult(Acc acc) {
        return acc;
    }

    @Override
    public Acc merge(Acc acc, Acc acc1) {
        return null;
    }
}
