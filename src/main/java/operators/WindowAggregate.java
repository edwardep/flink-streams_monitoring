package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.StreamType;
import datatypes.Vector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;

import static datatypes.InternalStream.windowSlide;

public class WindowAggregate<VectorType> extends KeyedProcessFunction<String, InternalStream, InternalStream> {

    private int window_size;
    private int window_slide;
    private BaseConfig<VectorType, ?> cfg;

    public WindowAggregate(int window_size, int window_slide, BaseConfig<VectorType, ?> cfg) {
        this.window_size = window_size;
        this.window_slide = window_slide;
        this.cfg = cfg;
    }

    private ListState<InternalStream> queue;
    @Override
    public void processElement(InternalStream input, Context ctx, Collector<InternalStream> out) throws Exception {
        // input contains a Vector with values aggregated from the most recent slide

        // retrieve previous queue state
        ArrayList<InternalStream> temp_queue = new ArrayList<>();
        for (InternalStream slide : queue.get())
            temp_queue.add(slide);

        // append new slide to queue
        temp_queue.add(input);

        VectorType slide_drift = null;
        // compute slide_drift = appending_slide - evicting slide || this needs to be tested
        // todo: this condition needs rework. In order to be correct it should compare timestamps...
        if (temp_queue.size() > window_size / window_slide) {
            slide_drift = cfg.subtractVectors((VectorType) input.getVector(), (VectorType) temp_queue.get(0).getVector());
            temp_queue.remove(0);
        }

        // output appending slide
        out.collect(windowSlide(ctx.getCurrentKey(), ctx.timestamp(), slide_drift));

        // save queue
        queue.update(temp_queue);

        // cleanup
        temp_queue.clear();
    }

    @Override
    public void open(Configuration parameters) {
        queue = getRuntimeContext()
                .getListState(new ListStateDescriptor<>("queue", TypeInformation.of(InternalStream.class)));
    }
}
