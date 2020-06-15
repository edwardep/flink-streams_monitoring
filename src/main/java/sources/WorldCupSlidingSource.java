package sources;

import datatypes.InputRecord;
import datatypes.WCStruct;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class WorldCupSlidingSource implements SourceFunction<InputRecord> {
    private String input_path;

    private boolean isRunning = true;

    private FileReader file;
    private FileReader file2;
    private BufferedReader reader;
    private BufferedReader reader2;

    private int windowSize;
    private long firstTs;

    public WorldCupSlidingSource(String input_path, int windowSize) {
        this.input_path = input_path;
        this.windowSize = windowSize*1000;
    }

    @Override
    public void run(SourceContext<InputRecord> sourceContext) throws Exception {
        file = new FileReader(input_path);
        file2 = new FileReader(input_path);
        reader = new BufferedReader(file);
        reader2 = new BufferedReader(file2);

        String[] tokens;
        if(reader2.ready()) {
            tokens = reader2.readLine().split(";");
            firstTs = Long.parseLong(tokens[WCStruct.timestamp.ordinal()])*1000;
        }

        int count = 0;

        while(isRunning) {

            if (reader.ready() && reader2.ready()) {
                tokens = reader.readLine().split(";");

                // multiplying by 1000 because world cup dataset has unix timestamps in seconds
                long timestampMillis = Long.parseLong(tokens[WCStruct.timestamp.ordinal()])*1000;

                /*
                 *  GenericInputStream Object:
                 *      timestamp = this is used by the timestamp extractor
                 *      streamId = all Worker-related keyBy() operations are done on this field
                 *      Tuple2.of( Tuple2.of(ClientID, request_type) ,  1.0 )   -> (key, val)
                 */
                InputRecord event = new InputRecord(
                        tokens[WCStruct.server.ordinal()],
                        timestampMillis,
                        Tuple2.of(
                                Integer.parseInt(tokens[WCStruct.clientID.ordinal()]),
                                Integer.parseInt(tokens[WCStruct.type.ordinal()])),
                        1d);

                sourceContext.collect(event);

                while(timestampMillis - firstTs >= windowSize) {
                    tokens = reader2.readLine().split(";");
                    firstTs = Long.parseLong(tokens[WCStruct.timestamp.ordinal()])*1000;

                    InputRecord evicting = new InputRecord(
                            tokens[WCStruct.server.ordinal()],
                            timestampMillis,
                            Tuple2.of(
                                    Integer.parseInt(tokens[WCStruct.clientID.ordinal()]),
                                    Integer.parseInt(tokens[WCStruct.type.ordinal()])),
                            -1d);
                    sourceContext.collect(evicting);

                    count++;
                }

                count++;
                if(count%100000 == 0) System.out.println(count);
            }
            else{
                cancel();
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
        try {
            file.close();
            file2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            reader.close();
            reader2.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
