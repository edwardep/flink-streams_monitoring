package operators;

import datatypes.InternalStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowFunction<VectorType> extends ProcessWindowFunction<VectorType, InternalStream, String, TimeWindow> {
    @Override
    public void process(String key, Context ctx, Iterable<VectorType> iterable, Collector<InternalStream> out) throws Exception {
        if(iterable.iterator().hasNext())
            out.collect(InternalStream.windowSlide(key, ctx.window().getEnd(), iterable.iterator().next()));
    }
}
