package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import fgm.CoordinatorFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state.CoordinatorStateHandler;


public class CoordinatorProcessFunction<VectorType, RecordType> extends CoProcessFunction<InternalStream, InternalStream, InternalStream> {

    private transient static Logger LOG = LoggerFactory.getLogger(CoordinatorProcessFunction.class);

    private CoordinatorStateHandler<VectorType, RecordType> state;
    private CoordinatorFunction<VectorType, RecordType> fgm;
    private BaseConfig<VectorType, RecordType> cfg;

    public CoordinatorProcessFunction(BaseConfig<VectorType, RecordType> cfg) {
        this.cfg = cfg;
    }

    @Override
    public void processElement1(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
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
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        fgm = new CoordinatorFunction<>(cfg);
        state = new CoordinatorStateHandler<>(getRuntimeContext(), cfg);
    }
}
