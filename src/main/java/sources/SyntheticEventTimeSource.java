package sources;

import datatypes.InputRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Random;

public class SyntheticEventTimeSource implements SourceFunction<InputRecord> {

    private boolean isRunning = true;
    private String streamID = "0";
    private int monotonicity = 0;
    private Random rand = new Random(512);
    private Random rand2 = new Random(1234);
    private int count = 0;

    private int records = 1000000;
    private int recordsPerSec = 10;
    private int keyRandomness = 100;
    private int gap = 5;
    private int groupSize = 10;

    public SyntheticEventTimeSource(int records,int recordsPerSec, int keyRandomness, int gap, int groupSize) {
        this.records = records;
        this.recordsPerSec = 1000/recordsPerSec;
        this.keyRandomness = keyRandomness;
        this.gap = gap;
        this.groupSize = groupSize;
    }

    public SyntheticEventTimeSource() {

    }

    @Override
    public void run(SourceContext<InputRecord> sourceContext) throws Exception {
        while(isRunning) {
            // Stop after..
            if(monotonicity > records) cancel();

            long timestampMillis = System.currentTimeMillis()+(recordsPerSec*monotonicity);
            InputRecord event = new InputRecord(
                    String.valueOf(rand2.nextInt(groupSize)), //streamID
                    timestampMillis,
                    Tuple2.of(rand.nextInt(keyRandomness),1),1d);


            monotonicity = monotonicity + rand.nextInt(gap) + 1;

            sourceContext.collect(event);
            count++;

            if(count % 10000 == 0) System.out.println("records produced: "+count);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}