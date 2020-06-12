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

public class WindowAggregate3<VectorType> extends KeyedProcessFunction<String, InternalStream, InternalStream> {

    private int window_size;
    private int window_slide;
    private BaseConfig<VectorType, ?> cfg;

    public WindowAggregate3(int window_size, int window_slide, BaseConfig<VectorType, ?> cfg) {
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
    private int count = 0;
    private long window_start;
    private long window_end;

    @Override
    public void processElement(InternalStream input, Context ctx, Collector<InternalStream> out) throws Exception {

        //System.out.println("newRecord @"+ctx.timestamp());

        if(first){
            window_start =  ctx.timestamp(); first = false; window_end = window_start + window_size;
        }

        if(ctx.timestamp() - lastTs > window_slide) {
            //System.out.println("GAP MANAGER");
            for (long start = lastTs + window_slide; start < ctx.timestamp(); start += window_slide) {
                //System.out.println("Start @ "+start);
                if (start >= window_end) {
                    window_start += window_slide;
                    window_end += window_slide;
                    //System.out.println("Window: ["+window_start+" : "+window_end+"]");

                    while (!queue.isEmpty() && queue.get(0).getTimestamp() < window_start)
                        queue.remove(0);
                }

                VectorType res = cfg.newInstance();
                for (InternalStream record : queue)
                    res = cfg.addVectors((VectorType) record.getVector(), res);

                out.collect(windowSlide("0", lastTs + start, res,0));

                //System.out.println("Queue contains the following:");
                //for(InternalStream elem : queue) System.out.println(elem.toString());

            }
        }

        queue.add(input);
        lastTs = ctx.timestamp();

        while(ctx.timestamp() - queue.get(0).getTimestamp() >= window_size - window_slide)
            queue.remove(0);

        VectorType res = cfg.newInstance();
        for (InternalStream record : queue)
            res = cfg.addVectors((VectorType) record.getVector(), res);

        out.collect(windowSlide("0", ctx.timestamp(), res,0));

        //System.out.println("Finally: ");
        //(InternalStream elem : queue) System.out.println(elem.toString());

        //System.out.println(++count);
    }

}
