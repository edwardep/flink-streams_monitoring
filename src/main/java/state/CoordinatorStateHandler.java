package state;

import configurations.BaseConfig;
import datatypes.Vector;
import fgm.SafeZone;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.IOException;

public class CoordinatorStateHandler<VectorType> extends StateHandler<VectorType>{

    private transient ValueState<VectorType> aggregateState;
    private transient ValueState<VectorType> estimate;
    private transient ValueState<SafeZone> safeZone;
    private transient ValueState<Boolean> sync;
    private transient ValueState<Integer> nodeCount;
    private transient ValueState<Double> psi;
    private transient ValueState<Double> psiBeta;
    private transient ValueState<Integer> globalCounter;

    private BaseConfig<?, VectorType, ?> cfg;

    public CoordinatorStateHandler(RuntimeContext runtimeContext, BaseConfig<?, VectorType, ?> cfg) {
        super(runtimeContext);
        this.cfg = cfg;
        init(cfg);
    }

    @Override
    public void init(BaseConfig<?, VectorType, ?> conf) {
        psi = createState("psiValue", Types.DOUBLE);
        psiBeta = createState("psiBeta", Types.DOUBLE);
        sync = createState("waitDrifts", Types.BOOLEAN);
        nodeCount = createState("nodeCount",Types.INT);
        globalCounter = createState("globalCounter", Types.INT);

        aggregateState = createState("aggregateState", conf.getVectorType());
        estimate = createState("estimate",conf.getVectorType());
        safeZone = createState("safeZone", TypeInformation.of(SafeZone.class));
    }

    /* Getters */
    public VectorType getEstimate() throws IOException {
        return estimate.value() != null ? estimate.value() : cfg.newInstance();
    }
    public VectorType getAggregateState() throws IOException {
        return aggregateState.value() != null ? aggregateState.value() : cfg.newInstance();
    }
    public SafeZone getSafeZone() throws IOException {
        return safeZone.value();
    }
    public Double getPsiBeta() throws IOException {
        return psiBeta.value() != null ? psiBeta.value() : 0d;
    }
    public Boolean getSync() throws IOException {
        return sync.value() != null ? sync.value() : true;
    }
    public Integer getNodeCount() throws IOException {
        return nodeCount.value() != null ? nodeCount.value() : 0;
    }
    public Double getPsi() throws IOException {
        return psi.value() != null ? psi.value() : 0d;
    }
    public Integer getGlobalCounter() throws IOException {
        return globalCounter.value() != null ? globalCounter.value() : 0;
    }

    /* Setters */
    public void setAggregateState(VectorType value) throws IOException { aggregateState.update(value); }
    public void setEstimate(VectorType value) throws IOException { estimate.update(value); }
    public void setSafeZone(SafeZone value) throws IOException { safeZone.update(value); }
    public void setGlobalCounter(Integer value) throws IOException { globalCounter.update(value); }
    public void setPsi(Double value) throws IOException { psi.update(value);}
    public void setNodeCount(Integer value) throws IOException { nodeCount.update(value);}
    public void setSync(Boolean value) throws IOException { sync.update(value); }
    public void setPsiBeta(Double value) throws IOException { psiBeta.update(value);}

}
