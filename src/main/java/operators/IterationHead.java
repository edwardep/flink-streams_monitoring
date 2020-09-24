package operators;

import datatypes.InternalStream;
import jobs.MonitoringJob;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

@Deprecated
public class IterationHead extends CoProcessFunction<InternalStream, InternalStream, InternalStream> {

    /**
     *  (input 1 : InputRecord) --> (main output : InputRecord)
     */
    @Override
    public void processElement1(InternalStream record, Context context, Collector<InternalStream> out) throws Exception {
        out.collect(record);
    }

    /**
     *  (input 2 : InternalStream(Feedback)) --> (side output : InternalStream)
     */
    @Override
    public void processElement2(InternalStream internalStream, Context context, Collector<InternalStream> out) throws Exception {
        context.output(MonitoringJob.feedback, internalStream);
    }
}
