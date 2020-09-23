package state;

import configurations.BaseConfig;
import fgm.SafeZone;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.IOException;
import java.util.UUID;

public class CoordinatorStateHandler<VectorType>{
    private final String UID = UUID.randomUUID().toString();

    private transient ValueState<VectorType> aggregateState;
    private transient ValueState<VectorType> estimate;
    private transient ValueState<SafeZone> safeZone;
    private transient ValueState<Boolean> sync;
    private transient ValueState<Integer> nodeCount;
    private transient ValueState<Double> psi;
    private transient ValueState<Double> psiBeta;
    private transient ValueState<Integer> globalCounter;
    private transient ValueState<Double> lambda;
    private transient ValueState<Long> lastTs;

    private IntCounter roundsCounter = new IntCounter();
    private IntCounter subroundsCounter = new IntCounter();
    private IntCounter rebalancedRoundsCounter = new IntCounter();

    private RuntimeContext runtimeContext;
    private BaseConfig<?, VectorType, ?> cfg;

    public CoordinatorStateHandler(RuntimeContext runtimeContext, BaseConfig<?, VectorType, ?> cfg) {
        this.cfg = cfg;
        this.runtimeContext = runtimeContext;

        psi = createState("psiValue", Types.DOUBLE);
        psiBeta = createState("psiBeta", Types.DOUBLE);
        sync = createState("waitDrifts", Types.BOOLEAN);
        nodeCount = createState("nodeCount",Types.INT);
        globalCounter = createState("globalCounter", Types.INT);

        aggregateState = createState("aggregateState", cfg.getVectorType());
        estimate = createState("estimate", cfg.getVectorType());
        safeZone = createState("safeZone", Types.POJO(SafeZone.class));
        lambda = createState("lambda", Types.DOUBLE);
        lastTs = createState("lastTas", Types.LONG);

        runtimeContext.addAccumulator("roundsCounter", this.roundsCounter);
        runtimeContext.addAccumulator("subroundsCounter", this.subroundsCounter);
        runtimeContext.addAccumulator("rebalancedRoundsCounter", this.rebalancedRoundsCounter);
    }

    private <V> ValueState<V> createState(String name, TypeInformation<V> type) {
        return runtimeContext
                .getState(new ValueStateDescriptor<>(UID+name, type));
    }

    /* Getters */
    public IntCounter getRoundsCounter() {
        return roundsCounter;
    }

    public IntCounter getSubroundsCounter() {
        return subroundsCounter;
    }

    public IntCounter getRebalancedRoundsCounter() {
        return rebalancedRoundsCounter;
    }

    public VectorType getEstimate() throws IOException {
        return estimate.value() != null ? estimate.value() : cfg.newVectorInstance();
    }
    public VectorType getAggregateState() throws IOException {
        return aggregateState.value() != null ? aggregateState.value() : cfg.newVectorInstance();
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
    public Double getLambda() throws IOException {
        return lambda.value() != null ? lambda.value() : 1.0;
    }
    public Long getLastTs() throws IOException {
        return lastTs.value() != null ? lastTs.value() : Long.MIN_VALUE;
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
    public void setLambda(Double value) throws IOException { lambda.update(value);}
    public void setLastTs(Long value) throws IOException { lastTs.update(value);}
}
