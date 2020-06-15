package test_utils;

import datatypes.InputRecord;
import jdk.internal.util.xml.impl.Input;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.Random;

public class SyntheticSlidingSource implements SourceFunction<InputRecord> {

    private boolean isRunning = true;
    private int streamID = 0;
    private int monotonicity = 0;
    private int bound = 100;

    private ArrayList<InputRecord> queue = new ArrayList<>();

    private int windowSize;
    private Random rand = new Random(512);

    int count = 0;

    public SyntheticSlidingSource(int windowSize) {
        this.windowSize = windowSize*1000;
    }

    @Override
    public void run(SourceContext<InputRecord> sourceContext) throws Exception {

        while(isRunning) {

            /*
             *  Creates 1 GenericInputStream object per streamID per second (event time & ascending)
             */
            long timestampMillis = 5L+(100*monotonicity);
            int key = rand.nextInt(100);

            InputRecord event = new InputRecord("0", timestampMillis, Tuple2.of(key, 1),1d);
            sourceContext.collect(event);
            queue.add(event);

            count++;

            // Stop after..
            if(monotonicity > 1000000)
                cancel();

            monotonicity = monotonicity +1;//+ rand.nextInt(50) + 1;
            //if(monotonicity > 2) monotonicity += 9;

            if(timestampMillis - queue.get(0).getTimestamp() >= windowSize){
                InputRecord evicting = queue.get(0);
                evicting.setVal(-evicting.getVal());
                evicting.setTimestamp(timestampMillis);
                sourceContext.collect(evicting);
                count++;
                queue.remove(0);
            }

            if(count % 10000 == 0)  System.out.println("SLI"+count);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}