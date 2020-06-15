package fgm;

import configurations.TestP1Config;
import configurations.TestP4Config;
import datatypes.InternalStream;
import datatypes.Vector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import state.CoordinatorStateHandler;
import test_utils.Testable;

import static datatypes.InternalStream.*;
import static junit.framework.TestCase.*;
import static test_utils.Generators.generateSequence;

public class CoordinatorFunction_test {

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

    @Test
    public void aggregatingEstimate_test() throws Exception {
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.CoordinatorFunction fgm;
            CoordinatorStateHandler state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);
                fgm.disableRebalancing();

                // mock input
                InternalStream input = downstreamDrift(0, new Vector(generateSequence(5)));

                // call routine
                fgm.handleDrift(state, input, ctx, collector);

                assertEquals(new Vector(generateSequence(5)), state.getEstimate());

                state.setNodeCount(0);
                // calling again, expecting the values to be doubled
                fgm.handleDrift(state, input, ctx, collector);

                assertEquals(new Vector(generateSequence(5,2)), state.getEstimate());

                state.setNodeCount(0);
                // mock negative drift values
                input = downstreamDrift(0, new Vector(generateSequence(5, -1)));
                fgm.handleDrift(state, input, ctx, collector);

                assertEquals(new Vector(generateSequence(5)), state.getEstimate());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.CoordinatorFunction<>(conf);
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        });
        env.execute();
    }

    /**
     * Testing the integrity of {@link fgm.CoordinatorFunction#handleDrift handleDrift()} method.
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void fgm_handleDrift_pass_test() throws Exception {
        int uniqueStreams = 4;

        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.CoordinatorFunction fgm;
            CoordinatorStateHandler state;
            TestP4Config conf = new TestP4Config(); // overriding setup configuration

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);
                fgm.disableRebalancing();

                /*
                 *  TEST_CASE: As drift updates arrive, the coordinator aggregates them,
                 *   computes the Global Estimate vector and finally broadcasts it back(hyperparameters).
                 */
                for (int tid = 0; tid < uniqueStreams; tid++) {
                    InternalStream payload = downstreamDrift(0, new Vector(generateSequence(10)));
                    fgm.handleDrift(state, payload, ctx, collector);
                }

                // validate : expecting global state to be equal to each drift since it is the result of Averaging
                assertEquals(new Vector(generateSequence(10)), state.getEstimate());
                assertEquals(state.getAggregateState(), conf.newInstance());
                assertFalse(state.getSync());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.CoordinatorFunction<>(conf);
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // checking broadcast
        for(int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = upstreamGlobalEstimate(String.valueOf(tid), new Vector(generateSequence(10)));
            assertEquals(expected.toString(), Testable.InternalStreamSink.result.get(tid).toString());
        }
    }

    /**
     * Testing the integrity of {@link fgm.CoordinatorFunction#handleZeta handleZeta()} method when &psi; &ge; &epsilon;k&phi;(0)
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void fgm_handleZeta_fail_test() throws Exception {
        int uniqueStreams = 4;

        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.CoordinatorFunction fgm;
            CoordinatorStateHandler state;
            TestP4Config conf = new TestP4Config(); // overriding setup configuration

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                Double zeta = 1.0;
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);
                fgm.disableRebalancing();

                // test_case: Workers send their Phi(X) values and coordinator computes Psi.
                for (int tid = 0; tid < uniqueStreams; tid++)
                    fgm.handleZeta(state, ctx, zeta, collector);

                // validate
                assertEquals(uniqueStreams * zeta, state.getPsi());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.CoordinatorFunction<>(conf);
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        for (int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = upstreamRequestDrift(String.valueOf(tid));
            assertEquals(expected.toString(), Testable.InternalStreamSink.result.get(tid).toString());
        }
    }

    /**
     * Testing the integrity of {@link fgm.CoordinatorFunction#handleZeta handleZeta()} method when &psi; &lt; &epsilon;k&phi;(0)
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void fgm_handleZeta_pass_test() throws Exception {
        // setup : mock received Phi(Xi) named zeta
        Double zeta = -1.0;
        int uniqueStreams = 4;
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.CoordinatorFunction fgm;
            CoordinatorStateHandler state;
            TestP4Config conf = new TestP4Config();

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);
                fgm.disableRebalancing();

                /*
                 *  TEST_CASE: The computed psi is less or equal than the Threshold, so a new subRound
                 *   should begin.
                 */
                for (int tid = 0; tid < uniqueStreams; tid++)
                    fgm.handleZeta(state, ctx, zeta, collector);

                // validate
                assertEquals(uniqueStreams * zeta, state.getPsi());

                // next phase would be 'increment processing' so sync must be disabled
                assertFalse(state.getSync());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.CoordinatorFunction<>(conf);
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        Double quantum = -(uniqueStreams * zeta) / (2 * uniqueStreams); // psi = zeta*uniqueStreams because zeta is constant
        for (int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = upstreamQuantum(String.valueOf(tid), quantum);
            assertEquals(expected.toString(), Testable.InternalStreamSink.result.get(tid).toString());
        }
    }

    /**
     * Testing the integrity of {@link fgm.CoordinatorFunction#handleIncrement handleIncrement()} method
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void fgm_handleIncrement_test() throws Exception {
        int uniqueStreams = 4;
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.CoordinatorFunction fgm;
            CoordinatorStateHandler state;
            TestP4Config conf = new TestP4Config();

            @Override
            public void processElement(InternalStream input, Context ctx, Collector<InternalStream> collector) throws Exception {
                /* Sync must be disabled for this test case. By default it is done by one of the other two functions */
                state.setSync(false);

                /*
                 * TEST_CASE:
                 *      Calling handleIncrement() with parallelism 4 and increment value 1.
                 *      Expecting to increase the global counter and do nothing
                 */
                Integer increment = 1;
                fgm.handleIncrement(state, increment, collector);
                assertEquals(increment, state.getGlobalCounter());
                assertFalse(state.getSync());

                /*
                 * TEST_CASE:
                 *      Calling handleIncrement() with parallelism 4 and increment value 5.
                 *      Expecting to increase the global counter which leads to sub-round violation.
                 *      In this case it must broadcast a ZetaRequest and disable sync.
                 */
                increment = 5; // obv this increment > uniqueStreams (k)
                fgm.handleIncrement(state, increment, collector);
                assertEquals((Integer) 0, state.getGlobalCounter());
                assertTrue(state.getSync());
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.CoordinatorFunction<>(conf);
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        for (int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = upstreamRequestZeta(String.valueOf(tid));
            assertEquals(expected.toString(), Testable.InternalStreamSink.result.get(tid).toString());
        }
        Testable.InternalStreamSink.result.clear();
    }

    /**
     * Testing many consecutive subRounds.
     *
     * @throws Exception any Flink exception
     */
    @Test
    public void fgm_multipleSubRounds_test() throws Exception {
        int uniqueStreams = 4;
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            fgm.CoordinatorFunction fgm;
            CoordinatorStateHandler state;
            TestP4Config conf = new TestP4Config();

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);

                for (int subRounds = 0; subRounds < 50; subRounds++) {
                    // receiving Phi(Xi), new SubRound
                    InternalStream stream = downstreamZeta(-1.0);
                    for (int i = 0; i < uniqueStreams; i++)
                        fgm.handleZeta(state, ctx, stream.getPayload(), collector);

                    assertFalse(state.getSync());

                    // receiving increment (violation), end of subRound
                    stream = downstreamIncrement(5.0);
                    fgm.handleIncrement(state, stream.getPayload().intValue(), collector);
                    assertTrue(state.getSync());
                }
            }

            @Override
            public void open(Configuration parameters) {
                fgm = new fgm.CoordinatorFunction<>(conf);
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        //for (int i = 0; i < Testable.InternalStreamSink.result.size(); i++)
            //System.out.println(Testable.InternalStreamSink.result.get(i).toString());
    }
}
