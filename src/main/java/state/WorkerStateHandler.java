package state;

import configurations.BaseConfig;
import datatypes.Vector;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.IOException;

public class WorkerStateHandler<VectorType, RecordType> extends StateHandler<VectorType, RecordType>{
    private transient ValueState<VectorType> driftVector;
    private transient ValueState<VectorType> estimate;

    private transient ValueState<Boolean> subRoundPhase;
    private transient ValueState<Boolean> subRoundInit;
    private transient ValueState<Double> zeta;
    private transient ValueState<Integer> localCounter;
    private transient ValueState<Double> fi;
    private transient ValueState<Double> quantum;

    private transient ValueState<Double> lastZeta;

    private transient ValueState<Double> lambda;
    private transient ValueState<Long> lastTs;

    private BaseConfig<VectorType, RecordType> cfg;

    public WorkerStateHandler(RuntimeContext runtimeContext, BaseConfig<VectorType, RecordType> cfg) {
        super(runtimeContext);
        this.cfg = cfg;
        init(cfg);
    }
    @Override
    public void init(BaseConfig<VectorType, RecordType> conf) {

        driftVector = createState("driftVector", conf.getVectorType());
        estimate = createState("estimate", conf.getVectorType());

        subRoundInit = createState("subRoundInit", Types.BOOLEAN);
        subRoundPhase = createState("subRoundPhase", Types.BOOLEAN);

        fi = createState("fi", Types.DOUBLE);
        zeta = createState("zeta", Types.DOUBLE);
        quantum = createState("quantum", Types.DOUBLE);
        localCounter = createState("localCounter", Types.INT);
        lastZeta = createState("lastZeta", Types.DOUBLE);
        lastTs = createState("lastTs", Types.LONG);
        lambda = createState("lambda", Types.DOUBLE);
    }

    /* Getters */

    public VectorType getDrift() throws IOException {
        return driftVector.value() != null ? driftVector.value() : cfg.newInstance();
    }
    public VectorType getEstimate() throws IOException {
        return estimate.value() != null ? estimate.value() : cfg.newInstance();
    }

    public Double getFi() throws IOException { return fi.value() != null ? fi.value() : 0d; }
    public Double getZeta() throws IOException { return zeta.value() != null ? zeta.value() : 0d; }
    public Double getQuantum() throws IOException { return quantum.value() != null ? quantum.value() : 0d; }

    public Boolean getSubRoundInit() throws IOException {
        return subRoundInit.value() != null ? subRoundInit.value() : false;
    }
    public Boolean getSubRoundPhase() throws IOException {
        return subRoundPhase.value() != null ? subRoundPhase.value() : false;
    }
    public Integer getLocalCounter() throws IOException {
        return localCounter.value() != null ? localCounter.value() : 0;
    }

    public Double getLastZeta() throws IOException { return lastZeta.value() != null ? lastZeta.value() : 0d; }

    public Long getLastTs() throws IOException { return  lastTs.value() != null ? lastTs.value() : 0L; }

    public Double getLambda() throws IOException { return  lambda.value() != null ? lambda.value() : 1.0; }

    /* Setters */
    public void setDrift(VectorType value) throws IOException { driftVector.update(value); }
    public void setEstimate(VectorType value) throws IOException { estimate.update(value); }

    public void setFi(Double value) throws IOException { fi.update(value);}
    public void setZeta(Double value) throws IOException { zeta.update(value);}
    public void setQuantum(Double value) throws IOException { quantum.update(value); }
    public void setSubRoundInit(Boolean value) throws IOException { subRoundInit.update(value); }
    public void setLocalCounter(Integer value) throws IOException { localCounter.update(value); }
    public void setSubRoundPhase(Boolean value) throws IOException { subRoundPhase.update(value); }

    public void setLastZeta(Double value) throws IOException { lastZeta.update(value);}

    public void setLastTs(Long value) throws IOException { lastTs.update(value); }

    public void setLambda(Double value) throws IOException { lambda.update(value); }

}
