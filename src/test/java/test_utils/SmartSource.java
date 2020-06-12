package test_utils;

import datatypes.InputRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SmartSource implements SourceFunction<InputRecord> {

    private boolean isRunning = true;
    private int streamID = 0;
    private int monotonicity = 0;
    private int bound = 100;
    private Random rand = new Random(512);
    private Random rand2 = new Random(1489);

    private int slide = 1;
    private int size;

    private List<InputRecord> queue = new ArrayList<>(size);

    public SmartSource(int windowSize) { this.size = windowSize; }
    @Override
    public void run(SourceContext<InputRecord> sourceContext) throws Exception {

        while(isRunning) {
            long timestampMillis = 1591892505000L+(1000*monotonicity);
            InputRecord event = new InputRecord(
                    "0",
                    timestampMillis,
                    Tuple2.of(rand.nextInt(100),1),1d);

            queue.add(event);

            // Stop after..
            if(monotonicity > 50000)
                cancel();

            monotonicity = monotonicity + rand.nextInt(20);
            sourceContext.collect(event);

            while(timestampMillis - queue.get(0).getTimestamp() > size*1000){
                InputRecord evicting = queue.get(0);
                evicting.setTimestamp(timestampMillis);
                evicting.setVal(-evicting.getVal());
                sourceContext.collect(evicting);
                queue.remove(0);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
