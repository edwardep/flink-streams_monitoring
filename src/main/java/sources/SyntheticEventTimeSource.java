package sources;

import datatypes.internals.Input;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SyntheticEventTimeSource implements SourceFunction<Input> {

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
        this.recordsPerSec = recordsPerSec;
        this.keyRandomness = keyRandomness;
        this.gap = gap;
        this.groupSize = groupSize;
    }

    public SyntheticEventTimeSource() {

    }

    @Override
    public void run(SourceContext<Input> sourceContext) throws Exception {
        long startTime = System.currentTimeMillis();
        while(isRunning) {
            // Stop after..
            if(monotonicity > records) cancel();

            long timestampMillis = startTime + ((1000 / recordsPerSec) * monotonicity);
            Input event = new Input(
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