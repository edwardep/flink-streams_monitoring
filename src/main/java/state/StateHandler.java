package state;

import configurations.BaseConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.util.UUID;

public abstract class StateHandler<VectorType> {

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
    public abstract void init(BaseConfig<?, VectorType, ?> conf);

    <V> ValueState<V> createState(String name, TypeInformation<V> type) {
        return runtimeContext
                .getState(new ValueStateDescriptor<>(UID+name, type));
    }

    <V> ListState<V> createListState(String name, TypeInformation<V> type) {
        return runtimeContext
                .getListState(new ListStateDescriptor<V>(UID+name, type));
    }
}
