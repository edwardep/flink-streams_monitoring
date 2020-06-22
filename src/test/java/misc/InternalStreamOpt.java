package misc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class InternalStreamOpt {

    @Test
    public void simplePipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(0, 1, 2)
                .map(new MapFunction<Integer, MockInternal>() {
                    @Override
                    public MockInternal map(Integer integer) throws Exception {
                        return MockInternal.sendDrift(integer);
                    }
                })
                .map(new MapFunction<MockInternal, Integer>() {
                    @Override
                    public Integer map(MockInternal mockInternal) throws Exception {
                        return (Integer)((MockDrift) mockInternal).getVec();
                    }
                })
                .print();

        env.execute();
    }
}
