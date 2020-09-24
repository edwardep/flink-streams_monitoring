package operators;

import configurations.BaseConfig;
import org.apache.flink.api.common.functions.AggregateFunction;

@Deprecated
public class IncAggregation<In, Acc> implements AggregateFunction<In, Acc, Acc> {

    private final BaseConfig<?> cfg;
    public IncAggregation(BaseConfig<?> cfg) { this.cfg = cfg; }

    @Override
    public Acc createAccumulator() {
        return null;//cfg.newAccInstance();
    }

    @Override
    public Acc add(In in, Acc acc) {
        return null;//cfg.aggregateRecord(in, acc);
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
