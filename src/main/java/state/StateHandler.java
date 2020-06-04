package state;

import configurations.BaseConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.UUID;

public abstract class StateHandler<VectorType, RecordType> {

    private String UID = UUID.randomUUID().toString();
    private RuntimeContext runtimeContext;

    public StateHandler(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    /**
     * Here you can initialize state variables.
     * Example: <code>mystate = createValueState("myState", config);</code>
     *
     * @param conf The FGM configuration class
     */
    public abstract void init(BaseConfig<VectorType, RecordType> conf);

    ValueState<VectorType> createState(String name, Class type) {
        return runtimeContext
                .getState(new ValueStateDescriptor<VectorType>(UID+name, Types.GENERIC(type)));
    }

    <V> ValueState<V> createState(String name, TypeInformation<V> type) {
        return runtimeContext
                .getState(new ValueStateDescriptor<>(UID+name, type));
    }
}
