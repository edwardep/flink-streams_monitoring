package operators;

import configurations.BaseConfig;
import datatypes.Accumulator;
import datatypes.InternalStream;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.UUID;


public class WindowFunction<AccType> extends ProcessWindowFunction<Accumulator<AccType>, Accumulator<AccType>, String, TimeWindow> {
    private String UID = UUID.randomUUID().toString();
    private BaseConfig<AccType,?,?> cfg;

    private transient ValueState<Accumulator> state;

    public WindowFunction(BaseConfig<AccType,?,?> cfg) {
        this.cfg = cfg;
    }

    @Override
    public void process(String key, Context ctx, Iterable<Accumulator<AccType>> iterable, Collector<Accumulator<AccType>> out) throws IOException {
        if(iterable.iterator().hasNext()){
            Accumulator accumulator = iterable.iterator().next();
            AccType drift;
            if(state.value() != null)
                drift = cfg.subtractAccumulators((AccType) accumulator.getVec(), (AccType) state.value().getVec());
            else
                drift = (AccType) accumulator.getVec();

            out.collect(new Accumulator<>(key, drift));
            state.update(accumulator);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = getRuntimeContext().getState(new ValueStateDescriptor<>(UID+"state", TypeInformation.of(Accumulator.class)));
    }
}
