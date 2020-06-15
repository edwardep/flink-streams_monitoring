package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import datatypes.StreamType;
import datatypes.Vector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.collection.immutable.Range;
import state.WindowStateHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static datatypes.InternalStream.windowSlide;

public class WindowProcess<VectorType> extends KeyedProcessFunction<String, InternalStream, InternalStream> {

    private int window_size;
    private int window_slide;
    private BaseConfig<VectorType, ?> cfg;

    public WindowProcess(int window_size, int window_slide, BaseConfig<VectorType, ?> cfg) {
        this.window_size = window_size*1000;  // timestamps are in milliseconds
        this.window_slide = window_slide*1000;
        this.cfg = cfg;
    }

    private WindowStateHandler<VectorType> state;

    @Override
    public void processElement(InternalStream input, Context ctx, Collector<InternalStream> out) throws Exception {

        //todo: A better version could be implemented that only emits the appending and evicting slides of the window

        long currentTs = ctx.timestamp() + 1;

        /*  Initialize window bounds when first slide arrives */
        if(state.init()){
            state.setWindowStart(currentTs);
            state.setWindowEnd(currentTs + window_size);
            state.endInit();
        }

        /*  Retrieve queue from state */
        ArrayList<InternalStream> queue = state.getQueue();

        /*  If no new slide arrived, simulate a window eviction */
        if(currentTs - state.getLastTs() > window_slide)
            handleTimeGap(queue, currentTs, ctx, out);

        /*  Enqueue arriving slide and save it's timestamp*/
        queue.add(input);
        state.setLastTs(currentTs);

        /*  Remove slides with timestamp outside the window bounds */
        while(currentTs - queue.get(0).getTimestamp() > window_size - window_slide)
            queue.remove(0);

        /*  Output window vector    */
        VectorType res = cfg.newInstance();
        for (InternalStream record : queue)
            res = cfg.addVectors((VectorType) record.getVector(), res);

        out.collect(windowSlide(ctx.getCurrentKey(), currentTs, res));

        /*  Save updated queue to state */
        state.setQueue(queue);
    }

    @Override
    public void open(Configuration parameters) {
        state = new WindowStateHandler<>(getRuntimeContext(), cfg);
    }

    /*  If the previous slide arrived earlier (> window_slide) than the current one, this subroutine will
     *   slide the window one slide at a time, remove the old elements and output the remaining ones.    */
    private void handleTimeGap(ArrayList<InternalStream> queue, long currentTs, Context ctx, Collector<InternalStream> out) throws IOException {
        for (long start = state.getLastTs() + window_slide; start <= currentTs; start += window_slide) {

            if (start >= state.getWindowEnd()) {
                // slide window
                state.setWindowStart(state.getWindowStart() + window_slide);
                state.setWindowEnd(state.getWindowEnd() + window_slide);

                // remove items with timestamps less than window_start timestamp
                while (!queue.isEmpty() && queue.get(0).getTimestamp() < state.getWindowStart())
                    queue.remove(0);

            }
            /* This is the last slide, it shouldn't output anything because it will be done outside the loop */
            if(start == currentTs) break;

            /*  Output window vector    */
            VectorType res = cfg.newInstance();
            for (InternalStream record : queue)
                res = cfg.addVectors((VectorType) record.getVector(), res);

            out.collect(windowSlide(ctx.getCurrentKey(), start, res));
        }
    }

}
