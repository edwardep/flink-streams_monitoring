package sources;

import configurations.BaseConfig;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.internals.Input;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WorldCupMapSource implements FlatMapFunction<String, InternalStream> {

    private BaseConfig<?,?,?> cfg;

    public WorldCupMapSource(BaseConfig<?,?,?> cfg) {
        this.cfg = cfg;
    }

    private int count = 0;

    @Override
    public void flatMap(String input, Collector<InternalStream> out) throws Exception {
        String[] tokens = input.split(";");

        // multiplying by 1000 because world cup dataset has unix timestamps in seconds
        long timestampMillis = Long.parseLong(tokens[WCStruct.timestamp.ordinal()])*1000;

        /*
         *  InputRecord Object:
         *      streamId = all Worker-related keyBy() operations are done on this field
         *      timestamp = this is used by the timestamp extractor
         *      Tuple2.of( Tuple2.of(ClientID, request_type) ,  1.0 )   -> (key, val)
         */
        Input event = new Input(
                hashStreamID(tokens[WCStruct.server.ordinal()]),
                timestampMillis,
                Tuple2.of(
                        Integer.parseInt(tokens[WCStruct.clientID.ordinal()]),
                        Integer.parseInt(tokens[WCStruct.type.ordinal()])),
                1d);

        out.collect(event);

        count++;
        if(count%100000 == 0) System.out.println(count);
    }

    private String hashStreamID(String streamID) {
        return String.valueOf(Integer.parseInt(streamID) % cfg.workers());
    }

    // these are the fields of the world cup data set. The enum is simply used for code readability.
    private enum WCStruct {
        timestamp,
        clientID,
        objectID,
        size,
        method,
        status,
        type,
        server
    }
}
