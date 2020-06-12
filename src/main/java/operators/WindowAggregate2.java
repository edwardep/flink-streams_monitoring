package operators;

import configurations.BaseConfig;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.StreamType;
import datatypes.Vector;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static datatypes.InternalStream.windowSlide;

public class WindowAggregate2<VectorType> extends KeyedProcessFunction<String, InternalStream, InternalStream> {

    private int window_size;
    private int window_slide;
    private BaseConfig<VectorType, ?> cfg;

    public WindowAggregate2(int window_size, int window_slide, BaseConfig<VectorType, ?> cfg) {
        this.window_size = window_size*1000;  // timestamps are in milliseconds
        this.window_slide = window_slide*1000;
        this.cfg = cfg;
    }

    //private ValueState<List<InternalStream>> queue;
    ArrayList<InternalStream> queue = new ArrayList<>();
    ArrayList<Long> timestamps = new ArrayList<>();

    private long lastTs;
    private int emptySlides = 0;
    private boolean first = true;
    @Override
    public void processElement(InternalStream input, Context ctx, Collector<InternalStream> out) throws Exception {
        // input contains a Vector with values aggregated from the most recent slide

        // this processElement was called by the timer.
        if(input == null) {
            System.out.println("~processElement_by_timer @ "+ctx.timestamp());
            try {
                System.out.println("~comparing to time(1): " + timestamps.get(0));
            }
            catch (IndexOutOfBoundsException exc)
            {
                throw new Exception();
            }
            if (ctx.timestamp() - timestamps.get(0) < window_slide) {
                System.out.println("DO_NOTHING: "+ (ctx.timestamp() - timestamps.get(0)));
                emptySlides = 0;
                timestamps.remove(0);
            }
            else
            {
                if(queue.size() == 0) return;
                System.out.println("EVICT: "+(ctx.timestamp() - timestamps.get(0)));
                if(timestamps.get(0) - queue.get(0).getTimestamp() - emptySlides*window_slide >= window_size) {
                    queue.remove(0);
                }
                timestamps.remove(0);

                VectorType res = cfg.newInstance();
                for (InternalStream record : queue)
                    res = cfg.addVectors((VectorType) record.getVector(), res);

                out.collect(windowSlide(ctx.getCurrentKey(), ctx.timestamp(), res, 0));
                //long fireTs = ctx.timestamp() + window_slide + 10;
                //System.out.println("~At "+ctx.timestamp()+" registering timer for "+fireTs);
                //ctx.timerService().registerEventTimeTimer(fireTs);
            }
            return;
        }
        ;
        // append new slide to queue
        queue.add(input);

        System.out.println("_processElement_by_event");
        for(InternalStream elem: queue)
            System.out.println(elem.toString());

        lastTs = ctx.timestamp();

        // compute slide_drift = appending_slide - evicting slide || this needs to be tested
        while(lastTs - queue.get(0).getTimestamp() >= window_size)
            queue.remove(0);

        VectorType res = cfg.newInstance();
        for(InternalStream record : queue)
            res = cfg.addVectors((VectorType) record.getVector(), res);

        out.collect(windowSlide(ctx.getCurrentKey(), ctx.timestamp(), res, 0));
        long fireTs = ctx.timestamp() + window_slide + 10;
        System.out.println("_At "+ctx.timestamp()+" registering timer for "+fireTs);
        if(first)
            first = false;
        else
            timestamps.add(lastTs);
        ctx.timerService().registerEventTimeTimer(fireTs);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InternalStream> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        emptySlides++;
        System.out.println("timer fired @"+timestamp);
        processElement(null, ctx, out);
    }
}
