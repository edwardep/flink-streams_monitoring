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
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.CoordinatorStateHandler;

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
        System.out.println("EOF:"+endOfFile+", CWM:"+ctx.timerService().currentWatermark());
        if(endOfFile && ctx.timerService().currentWatermark() == Long.MAX_VALUE)
            broadcast_SigInt(collector, cfg);

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

    }

    @Override
    public void processElement2(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
        // here you could initialize the globalEstimate
    }

    @Override
    public void open(Configuration parameters) {
        state = new CoordinatorStateHandler<>(getRuntimeContext(), cfg);
    }
}
