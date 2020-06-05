package sources;

import datatypes.InputRecord;
import datatypes.WCStruct;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class WorldCupSource implements SourceFunction<InputRecord> {
    private String input_path;

    private boolean isRunning = true;

    private FileReader file;
    private BufferedReader reader;

    public WorldCupSource(String input_path) {
        this.input_path = input_path;
    }

    @Override
    public void run(SourceContext<InputRecord> sourceContext) throws Exception {
        file = new FileReader(input_path);
        reader = new BufferedReader(file);

        int count = 0;

        while(isRunning) {

            if (reader.ready()) {
                String[] tokens = reader.readLine().split(";");

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
                        new Tuple2<>(
                                Tuple2.of(
                                        Integer.parseInt(tokens[WCStruct.clientID.ordinal()]),
                                        Integer.parseInt(tokens[WCStruct.type.ordinal()])),
                                1d));

                sourceContext.collect(event);

                count++;
                if(count%100000 == 0)
                    System.out.println(count);
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
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
