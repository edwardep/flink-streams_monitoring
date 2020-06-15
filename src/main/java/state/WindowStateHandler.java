package state;

import configurations.BaseConfig;
import datatypes.InternalStream;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WindowStateHandler<VectorType> extends StateHandler<VectorType>{

    private transient ListState<InternalStream> queue;
    private transient ValueState<Boolean> first;
    private transient ValueState<Long> lastTs;
    private transient ValueState<Long> windowStart;
    private transient ValueState<Long> windowEnd;

    private BaseConfig<VectorType, ?> cfg;

    public WindowStateHandler(RuntimeContext runtimeContext, BaseConfig<VectorType, ?> cfg) {
        super(runtimeContext);
        this.cfg = cfg;
        init(cfg);
    }
    @Override
    public void init(BaseConfig<VectorType, ?> conf) {

        queue = createListState("queue", TypeInformation.of(InternalStream.class));

        first = createState("first", Types.BOOLEAN);
        windowStart = createState("windowStart", Types.LONG);
        windowEnd = createState("windowEnd", Types.LONG);
        lastTs = createState("lastTs", Types.LONG);
    }

    /* Getters */

    public ArrayList<InternalStream> getQueue() throws Exception {
        ArrayList<InternalStream> res = new ArrayList<>();
        for(InternalStream e : queue.get()) res.add(e);
        return res;
    }

    public boolean init() throws IOException { return  first.value() != null ? first.value() : true; }

    public Long getWindowStart() throws IOException {
        return windowStart.value() != null ? windowStart.value() : 0L;
    }
    public Long getWindowEnd() throws IOException {
        return windowEnd.value() != null ? windowEnd.value() : 0L;
    }
    public Long getLastTs() throws IOException {
        return lastTs.value() != null ? lastTs.value() : 0L;
    }


    /* Setters */
    public void setQueue(ArrayList<InternalStream> value) throws Exception { queue.update(value); }

    public void endInit() throws IOException { first.update(false); }
    public void setLastTs(Long value) throws IOException { lastTs.update(value); }
    public void setWindowStart(Long value) throws IOException { windowStart.update(value); }
    public void setWindowEnd(Long value) throws IOException { windowEnd.update(value); }

}
