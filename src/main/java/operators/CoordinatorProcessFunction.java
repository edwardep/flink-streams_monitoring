package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.internals.Drift;
import datatypes.internals.Increment;
import datatypes.internals.InitCoordinator;
import datatypes.internals.Zeta;
import fgm.CoordinatorFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.CoordinatorStateHandler;


public class CoordinatorProcessFunction<VectorType> extends CoProcessFunction<InternalStream, InternalStream, InternalStream> {

    private transient static Logger LOG = LoggerFactory.getLogger(CoordinatorProcessFunction.class);

    private CoordinatorStateHandler<VectorType> state;
    private CoordinatorFunction<VectorType> fgm;
    private BaseConfig<?, VectorType, ?> cfg;

    public CoordinatorProcessFunction(BaseConfig<?, VectorType, ?> cfg) {
        this.cfg = cfg;
    }

    @Override
    public void processElement1(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
        //System.out.println(input.getClass());
        if (Drift.class.equals(input.getClass())) {
            fgm.handleDrift(state, (Drift<VectorType>) input, ctx, collector);
        } else if (Zeta.class.equals(input.getClass())) {
            fgm.handleZeta(state, ctx, ((Zeta) input).getPayload(), collector);
        } else if (Increment.class.equals(input.getClass())) {
            fgm.handleIncrement(state, ((Increment) input).getPayload(), collector);
        }
    }

    @Override
    public void processElement2(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
        // here you can initialize the globalEstimate
        fgm.disableRebalancing();
        long current = ctx.timerService().currentProcessingTime();
        ctx.timerService().registerProcessingTimeTimer(current + ((InitCoordinator)input).getWarmup()*1000);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fgm = new CoordinatorFunction<>(cfg);
        state = new CoordinatorStateHandler<>(getRuntimeContext(), cfg);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InternalStream> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        fgm.broadcast_RequestDrift(out);
    }
}
