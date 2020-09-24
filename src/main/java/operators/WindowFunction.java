package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.internals.WindowSlide;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

@Deprecated
public class WindowFunction<Acc> extends ProcessWindowFunction<Acc, InternalStream, String, TimeWindow> {

    private BaseConfig<?> cfg;

    private transient ValueState<Acc> state;

    public WindowFunction(BaseConfig<?> cfg) {
        this.cfg = cfg;
    }
    @Override
    public void process(String key, Context context, Iterable<Acc> iterable, Collector<InternalStream> out) throws Exception {
        if(iterable.iterator().hasNext()){
            Acc accumulator = iterable.iterator().next();
            Acc window_drift;
//            if(state.value() != null)
//                window_drift = cfg.subtractAccumulators(accumulator, state.value());
//            else
//                window_drift = accumulator;

            //out.collect(new WindowSlide<>(key, window_drift));
            state.update(accumulator);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //state = getRuntimeContext().getState(new ValueStateDescriptor<>("window", cfg.getAccType()));
    }
}
