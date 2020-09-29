package utils;

import datatypes.InternalStream;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class MaxWatermark implements AssignerWithPeriodicWatermarks<InternalStream> {

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(Long.MAX_VALUE);
    }

    @Override
    public long extractTimestamp(InternalStream internalStream, long l) {
        return 0;
    }
}
