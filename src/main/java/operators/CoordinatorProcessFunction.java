package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.StreamType;
import fgm.CoordinatorFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.CoordinatorStateHandler;


public class CoordinatorProcessFunction<VectorType> extends CoProcessFunction<InternalStream, InternalStream, InternalStream> {

    private transient static Logger LOG = LoggerFactory.getLogger(CoordinatorProcessFunction.class);

    private CoordinatorStateHandler<VectorType> state;
    private CoordinatorFunction<VectorType> fgm;
    private BaseConfig<VectorType, ?> cfg;

    public CoordinatorProcessFunction(BaseConfig<VectorType, ?> cfg) {
        this.cfg = cfg;
    }

    @Override
    public void processElement1(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
        //System.out.println(input.toString());
        switch(input.getType()) {
            case DRIFT:
                fgm.handleDrift(state, input, ctx, collector);
                break;
            case ZETA:
                fgm.handleZeta(state, ctx, input.getPayload(), collector);
                break;
            case INCREMENT:
                fgm.handleIncrement(state, input.getPayload().intValue(), collector);
                break;
        }
    }

    @Override
    public void processElement2(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
        // here you can initialize the globalEstimate
        fgm.disableRebalancing();
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + (input.getTimestamp()));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fgm = new CoordinatorFunction<>(cfg);
        state = new CoordinatorStateHandler<>(getRuntimeContext(), cfg);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InternalStream> collector) throws Exception {
        super.onTimer(timestamp, ctx, collector);
        fgm.broadcast_RequestDrift(collector);
    }
}
