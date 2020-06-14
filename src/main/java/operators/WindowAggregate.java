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
import java.util.ArrayList;

import static datatypes.InternalStream.windowSlide;

public class WindowAggregate<VectorType> extends KeyedProcessFunction<String, InternalStream, InternalStream> {

    private int window_size;
    private int window_slide;
    private BaseConfig<VectorType, ?> cfg;

    public WindowAggregate(int window_size, int window_slide, BaseConfig<VectorType, ?> cfg) {
        this.window_size = window_size*1000;  // timestamps are in milliseconds
        this.window_slide = window_slide*1000;
        this.cfg = cfg;
    }


    private ArrayList<InternalStream> queue = new ArrayList<>();

    private long lastTs;
    private boolean first = true;
    private long window_start;
    private long window_end;

    @Override
    public void processElement(InternalStream input, Context ctx, Collector<InternalStream> out) throws Exception {

        long currentTs = ctx.timestamp() + 1;

        if(first){
            window_start = currentTs; window_end = window_start + window_size; first = false;
        }

        /*  If the previous slide arrived earlier (> window_slide) than the current one, this subroutine will
        *   slide the window one slide at a time, remove the old elements and output the remaining ones.    */
        if(currentTs - lastTs > window_slide) {

            for (long start = lastTs + window_slide; start <= currentTs; start += window_slide) {

                if (start >= window_end) {
                    // slide window
                    window_start += window_slide;
                    window_end += window_slide;

                    // remove items with timestamps less than window_start timestamp
                    while (!queue.isEmpty() && queue.get(0).getTimestamp() < window_start)
                        queue.remove(0);
                }
                /* This is the last slide, it shouldn't output anything because it will be done outside the loop */
                if(start == currentTs) break;

                /*  Output window vector    */
                VectorType res = cfg.newInstance();
                for (InternalStream record : queue)
                    res = cfg.addVectors((VectorType) record.getVector(), res);

                out.collect(windowSlide("0", start, res,0));
            }
        }
        /*  Enqueue arriving slide and save it's timestamp*/
        queue.add(input);
        lastTs = currentTs;

        /*  Remove slides with timestamp outside the window limits */
        while(currentTs - queue.get(0).getTimestamp() > window_size - window_slide)
            queue.remove(0);

        /*  Output window vector    */
        VectorType res = cfg.newInstance();
        for (InternalStream record : queue)
            res = cfg.addVectors((VectorType) record.getVector(), res);

        out.collect(windowSlide("0", currentTs, res,0));

    }

    @Override
    public void open(Configuration parameters) {

    }
}
