package utils;

import datatypes.InternalStream;
import datatypes.internals.Input;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

import static jobs.MonitoringJobWithKafka.endOfFile;

public class AscendingWatermark implements AssignerWithPeriodicWatermarks<InternalStream> {


    long currentTimestamp = Long.MIN_VALUE;

    @Override
    public Watermark getCurrentWatermark() {
        return (endOfFile) ? new Watermark(Long.MAX_VALUE) : new Watermark(currentTimestamp);
    }

    @Override
    public long extractTimestamp(InternalStream internalStream, long l) {
        long newTimestamp = ((Input)internalStream).getTimestamp();
        if(endOfFile)
            return Long.MAX_VALUE;

        if (newTimestamp >= this.currentTimestamp)
            this.currentTimestamp = newTimestamp;
        return newTimestamp;
    }
}
