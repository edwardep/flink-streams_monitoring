package test_utils;

import datatypes.InputRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class SyntheticEventTimeSource implements SourceFunction<InputRecord> {

    private boolean isRunning = true;
    private int streamID = 0;
    private int monotonicity = 0;
    private int bound = 100;
    private Random rand = new Random(512);
    private Random rand2 = new Random(1489);
    @Override
    public void run(SourceContext<InputRecord> sourceContext) throws Exception {

        while(isRunning) {

            /*
             *  Creates 1 GenericInputStream object per streamID per second (event time & ascending)
             */
            long timestampMillis = System.currentTimeMillis()+(1000*monotonicity);
            InputRecord event = new InputRecord(
                    "0",
                    timestampMillis,
                    Tuple2.of(monotonicity,1),1d);


            //streamID++;

            // Stop after..
            if(monotonicity > 50000)
                cancel();

            // manipulating the (Q(S) and Q(E)) curves height by changing the randomness bound on the keys
//            if(monotonicity < 1000*10)
//                bound  = 1000;
//            else if(monotonicity < 1000*15)
//                bound = 500;
//            else if(monotonicity < 1000*20)
//                bound = 1200;
//            else if(monotonicity < 1000*25)
//                bound = 500;
//
//            if(monotonicity%10000 == 0)
//                System.out.println(monotonicity);

            // number of unique streams
//            if(streamID == 25) {
//                streamID = 0;
//                monotonicity++;
            //}
            monotonicity = monotonicity + rand.nextInt(10)+1;
            sourceContext.collect(event);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}