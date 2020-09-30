package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.Drift;
import datatypes.internals.Increment;
import datatypes.internals.InitCoordinator;
import datatypes.internals.Zeta;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.CoordinatorStateHandler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static fgm.CoordinatorFunction.*;
import static jobs.MonitoringJobWithKafka.endOfFile;


public class CoordinatorProcessFunction<VectorType> extends CoProcessFunction<InternalStream, InternalStream, InternalStream> {

    private transient static Logger LOG = LoggerFactory.getLogger(CoordinatorProcessFunction.class);

    private CoordinatorStateHandler<VectorType> state;
    private final BaseConfig<VectorType> cfg;

    public CoordinatorProcessFunction(BaseConfig<VectorType> cfg) {
        this.cfg = cfg;
    }

    @Override
    public void processElement1(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
        //System.out.println(input.getClass());

        switch (input.type){
            case "Drift":
                handleDrift(state, (Drift<VectorType>) input, ctx, collector, cfg);
                break;
            case "Zeta":
                handleZeta(state, ((Zeta) input).getPayload(), collector, cfg);
                break;
            case "Increment":
                handleIncrement(state, ((Increment) input).getPayload(), collector, cfg);
                break;
        }

        resetTimeoutTimer(ctx.timerService().currentWatermark(), state, ctx, collector, cfg);
    }

    @Override
    public void processElement2(InternalStream input, Context ctx, Collector<InternalStream> collector) {
        // here you could initialize the globalEstimate
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InternalStream> out) {
        broadcast_SigInt(out, cfg);
    }

    @Override
    public void open(Configuration parameters) {
        state = new CoordinatorStateHandler<>(getRuntimeContext(), cfg);
    }
}
