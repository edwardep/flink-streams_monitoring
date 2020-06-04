package jobs;

import datatypes.InternalStream;
import org.apache.flink.util.OutputTag;

public class MonitoringJob {

    public static final OutputTag<String> Q_estimate = new OutputTag<String>("estimate-side-output"){};
    public static final OutputTag<InternalStream> feedback = new OutputTag<InternalStream>("feedback-side-input"){};

}
