package operators;

import datatypes.InputRecord;
import datatypes.InternalStream;
import jobs.MonitoringJob;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class IterationHead extends CoProcessFunction<InputRecord, InternalStream, InputRecord> {

    /**
     *  (input 1 : InputRecord) --> (main output : InputRecord)
     */
    @Override
    public void processElement1(InputRecord record, Context context, Collector<InputRecord> out) throws Exception {
        out.collect(record);
    }

    /**
     *  (input 2 : InternalStream(Feedback)) --> (side output : InternalStream)
     */
    @Override
    public void processElement2(InternalStream internalStream, Context context, Collector<InputRecord> out) throws Exception {
        context.output(MonitoringJob.feedback, internalStream);
    }
}
