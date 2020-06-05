package operators;

import configurations.BaseConfig;
import datatypes.InternalStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static datatypes.InternalStream.slideAggregate;


public class SlideAggregate<VectorType, RecordType> extends ProcessWindowFunction<RecordType, InternalStream, String, TimeWindow> {

    private BaseConfig<VectorType, RecordType> cfg;
    public SlideAggregate(BaseConfig<VectorType, RecordType> cfg) { this.cfg = cfg; }

    @Override
    public void process(String streamID, Context ctx, Iterable<RecordType> iterable, Collector<InternalStream> out) {
        VectorType vector = cfg.batchUpdate(iterable);
        out.collect(slideAggregate(streamID, vector));
    }
}
