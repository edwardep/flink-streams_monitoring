package fgm;


import datatypes.InternalStream;
import datatypes.Vector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import state.WorkerStateHandler;
import test_utils.TestP1Config;
import test_utils.Testable;


import java.util.HashMap;


import static datatypes.InternalStream.emptyStream;
import static junit.framework.TestCase.*;
import static test_utils.Generators.generateSequence;

public class WorkerFunction_test {
    private StreamExecutionEnvironment env;
    private KeyedStream<InternalStream, String> source;

    // instantiate fgm configuration
    private static TestP1Config conf = new TestP1Config();

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        source = env.fromElements(emptyStream()).keyBy(InternalStream::getStreamID);
    }

    @After
    public void tearUp() {
        Testable.InternalStreamSink.result.clear();
    }


    /**
     * Testing the integrity of {@link fgm.WorkerFunction#updateDrift   updateDrift()} method
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void updateDrift_test() throws Exception {
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.WorkerFunction fgm;
            WorkerStateHandler state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {

                // mock input
                Vector batchUpdate = new Vector(generateSequence(5));

                // test_case: previous drift is empty (checking ValueState null return)
                fgm.updateDrift(state, batchUpdate);

                for(int i = 0; i < 5; i++)
                    assert ((Vector) state.getDrift()).getValue(Tuple2.of(i,i)) == i;

                // test_case : previous drift is not empty
                Vector prevDrift =  new Vector(generateSequence(5));
                state.setDrift(prevDrift);

                // retry
                fgm.updateDrift(state, batchUpdate);

                for(int i = 0; i < 5; i++)
                    assert ((Vector) state.getDrift()).getValue(Tuple2.of(i,i)) == i*2;
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.WorkerFunction<>(conf);
                state = new WorkerStateHandler<>(getRuntimeContext(), conf);
            }
        });
        env.execute();
    }

    /**
     * Testing the integrity of {@link fgm.WorkerFunction#newSubRound  newSubRound()} method
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void newSubRound_test() throws Exception {
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.WorkerFunction fgm;
            WorkerStateHandler state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                // setup : mock quantum (payload)
                Double receivedQuantum = 1.5;

                // test_case: handle the received Quantum value and restart the sub-round phase
                fgm.newSubRound(state, receivedQuantum);

                // validate
                assertEquals(1.5, state.getQuantum());
                assertTrue(state.getSubRoundInit());
                assertTrue(state.getSubRoundPhase());

            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.WorkerFunction<>(conf);
                state = new WorkerStateHandler<>(getRuntimeContext(), conf);
            }
        });
        env.execute();
    }

    /**
     * Testing the integrity of {@link fgm.WorkerFunction#newRound newRound()} method
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void newRound_test() throws Exception {
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.WorkerFunction fgm;
            WorkerStateHandler state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                // setup : mock estimate (hyperparams payload)
                HashMap<Tuple2<Integer, Integer>, Double> est = new HashMap<>();
                est.put(Tuple2.of(0,1), 10d);
                Vector hyperparams = new Vector(est);

                // test_case: Hyperparameters are received and a new Round begins
                fgm.newRound(state, hyperparams);

                // validate
                assertEquals("{(0,1)=10.0}", state.getEstimate().toString());
                assertEquals(1.0, state.getQuantum());
                assertEquals(conf.safeFunction(null, (Vector) state.getEstimate()), state.getLastZeta());
                assertTrue(state.getSubRoundPhase());
                assertTrue(state.getSubRoundInit());

            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.WorkerFunction<>(conf);
                state = new WorkerStateHandler<>(getRuntimeContext(), conf);
            }
        });
        env.execute();
    }

    /**
     * Testing the integrity of {@link fgm.WorkerFunction#subRoundProcess subRoundProcess()} method
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void subRoundProcess_test() throws Exception {
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.WorkerFunction fgm;
            WorkerStateHandler state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                // setup : initialize estimate, so that phi(X) has non zero value
                HashMap<Tuple2<Integer,Integer>, Double> est = new HashMap<>();
                est.put(Tuple2.of(0,1), 10d);
                state.setEstimate(new Vector(est));

                // test_case: SubRound phase is not yet enabled
                fgm.subRoundProcess(state, collector);

                // early exit (first condition)
                assertEquals(0.0, state.getFi());

                // enabling subRound phase
                state.setSubRoundPhase(true);

                // test_case: Drift is empty, should exit immediately
                fgm.subRoundProcess(state, collector);

                // early exit (second condition)
                assertEquals(0.0, state.getFi());

                // setup: at some point in time..
                HashMap<Tuple2<Integer,Integer>, Double> map = new HashMap<>();
                map.put(Tuple2.of(0,1), 0.5);
                state.setDrift(new Vector(map));
                state.setZeta(-3.0);
                state.setQuantum(1.0);
                state.setSubRoundInit(false);

                // test_case: A drift update occurred. Expecting the new counter to increase by 1, and phi < -1.0
                fgm.subRoundProcess(state, collector);

                // validate
                assertEquals(-1.5, state.getFi());
                assertEquals((Integer) 1, state.getLocalCounter());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.WorkerFunction<>(conf);
                state = new WorkerStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // test_case: assert that the emitted object is the expected one
        InternalStream expected = InternalStream.downstreamIncrement(1d);
        assertEquals(expected.toString(), Testable.InternalStreamSink.result.get(0).toString());
    }

    /**
     * Testing the integrity of {@link fgm.WorkerFunction#sendDrift sendDrift()} method
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void sendDrift_test() throws Exception {
        // mock drift vector
        HashMap<Tuple2<Integer, Integer>, Double> mock = generateSequence(10);

        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.WorkerFunction fgm;
            WorkerStateHandler state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                state.setDrift(new Vector(mock));

                // test_case: At the end of a Round, Nodes are requested to sent their driftVectors
                fgm.sendDrift(state, collector);

                // validate: vector should be empty after the previous function
                assertEquals(state.getDrift(), conf.newInstance());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.WorkerFunction<>(conf);
                state = new WorkerStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // test_case: assert that the emitted object is the expected one
        InternalStream expected = InternalStream.downstreamDrift(0, new Vector(mock));
        assertEquals(expected.toString(), Testable.InternalStreamSink.result.get(0).toString());
    }

    /**
     * Testing the integrity of {@link fgm.WorkerFunction#sendZeta sendZeta()} method
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void sendZeta_test() throws Exception {
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.WorkerFunction fgm;
            WorkerStateHandler state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                state.setFi(-5.0);

                // test_case: At the end of a SubRound nodes are requested to send their current Phi(Xi)
                fgm.sendZeta(state, collector);

                // validate
                assertFalse(state.getSubRoundPhase());
                assertEquals(-5.0, state.getLastZeta());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.WorkerFunction<>(conf);
                state = new WorkerStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // test_case: assert that the emitted object is the expected one
        InternalStream expected = InternalStream.downstreamZeta(-5.0);
        assertEquals(expected.toString(), Testable.InternalStreamSink.result.get(0).toString());
    }

}
