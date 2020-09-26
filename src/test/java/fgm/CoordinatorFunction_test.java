package fgm;

import configurations.TestP1Config;
import configurations.TestP4Config;
import datatypes.InternalStream;
import datatypes.Vector;
import datatypes.internals.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import state.CoordinatorStateHandler;
import test_utils.Testable;

import static fgm.CoordinatorFunction.*;
import static junit.framework.TestCase.*;
import static test_utils.Generators.generateSequence;

public class CoordinatorFunction_test {

    private StreamExecutionEnvironment env;
    private KeyedStream<InternalStream, String> source;

    // instantiate fgm configuration
    private static final TestP1Config conf = new TestP1Config();

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        source = env
                .addSource(new SourceFunction<InternalStream>() {
                    @Override
                    public void run(SourceContext<InternalStream> sourceContext) throws Exception {
                        sourceContext.collect(new EmptyStream());
                    }
                    @Override
                    public void cancel() { }
                })
                .keyBy(InternalStream::unionKey);
    }

    @After
    public void tearUp() {
        Testable.InternalStreamSink.result.clear();
    }

    @Test
    public void aggregatingEstimate_test() throws Exception {
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            CoordinatorStateHandler<Vector> state;

            @Override
            public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);

                // mock input
                Drift<Vector> input = new Drift<>(0, new Vector(generateSequence(5)));

                // call routine
                handleDrift(state, input, ctx, collector, conf);

                assertEquals(new Vector(generateSequence(5)), state.getEstimate());

                state.setNodeCount(0);
                // calling again, expecting the values to be doubled
                handleDrift(state, input, ctx, collector, conf);

                assertEquals(new Vector(generateSequence(5,2)), state.getEstimate());

                state.setNodeCount(0);
                // mock negative drift values
                input = new Drift<>(0, new Vector(generateSequence(5, -1)));
                handleDrift(state, input, ctx, collector, conf);

                assertEquals(new Vector(generateSequence(5)), state.getEstimate());
            }

            @Override
            public void open(Configuration parameters) {
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
            CoordinatorStateHandler<Vector> state;
            final TestP4Config conf = new TestP4Config(); // overriding setup configuration

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);

                /*
                 *  TEST_CASE: As drift updates arrive, the coordinator aggregates them,
                 *   computes the Global Estimate vector and finally broadcasts it back(hyperparameters).
                 */
                for (int tid = 0; tid < uniqueStreams; tid++) {
                    Drift<Vector> payload = new Drift<>(0, new Vector(generateSequence(10)));
                    handleDrift(state, payload, ctx, collector, conf);
                }

                // validate : expecting global state to be equal to each drift since it is the result of Averaging
                assertEquals(new Vector(generateSequence(10)), state.getEstimate());
                assertEquals(state.getAggregateState(), conf.newVectorInstance());
                assertFalse(state.getSync());
            }

            @Override
            public void open(Configuration parameters) {
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // checking broadcast
        for(int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = new GlobalEstimate<>(String.valueOf(tid), new Vector(generateSequence(10)));
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
            CoordinatorStateHandler<Vector> state;
            final TestP4Config conf = new TestP4Config(); // overriding setup configuration

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                Double zeta = -1.0;
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);
                state.setEstimate(new Vector(generateSequence(5)));

                // test_case: Workers send their Phi(X) values and coordinator computes Psi.
                for (int tid = 0; tid < uniqueStreams; tid++)
                    handleZeta(state, zeta, collector, conf);

                // validate
                assertEquals(uniqueStreams * zeta, state.getPsi());
            }

            @Override
            public void open(Configuration parameters) {
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        for (int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = new RequestDrift(String.valueOf(tid));
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
        double zeta = 1.0;
        int uniqueStreams = 4;
        source.process(new KeyedProcessFunction<String, InternalStream, InternalStream>() {
            CoordinatorStateHandler<Vector> state;
            final TestP4Config conf = new TestP4Config();

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                // setup
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);
                state.setEstimate(new Vector(generateSequence(5)));

                /*
                 *  TEST_CASE: The computed psi is less or equal than the Threshold, so a new subRound
                 *   should begin.
                 */
                for (int tid = 0; tid < uniqueStreams; tid++)
                    handleZeta(state, zeta, collector, conf);

                // validate
                assertEquals(uniqueStreams * zeta, state.getPsi());

                // next phase would be 'increment processing' so sync must be disabled
                assertFalse(state.getSync());
            }

            @Override
            public void open(Configuration parameters) {
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        double quantum = (uniqueStreams * zeta) / (2 * uniqueStreams); // psi = zeta*uniqueStreams because zeta is constant
        for (int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = new Quantum(String.valueOf(tid), quantum);
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
            CoordinatorStateHandler<Vector> state;
            final TestP4Config conf = new TestP4Config();

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
                handleIncrement(state, increment, collector, conf);
                assertEquals(increment, state.getGlobalCounter());
                assertFalse(state.getSync());

                /*
                 * TEST_CASE:
                 *      Calling handleIncrement() with parallelism 4 and increment value 5.
                 *      Expecting to increase the global counter which leads to sub-round violation.
                 *      In this case it must broadcast a ZetaRequest and disable sync.
                 */
                increment = 5; // obv this increment > uniqueStreams (k)
                handleIncrement(state, increment, collector, conf);
                assertEquals((Integer) 0, state.getGlobalCounter());
                assertTrue(state.getSync());
            }

            @Override
            public void open(Configuration parameters) {
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        for (int tid = 0; tid < uniqueStreams; tid++) {
            InternalStream expected = new RequestZeta(String.valueOf(tid));
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
            CoordinatorStateHandler<Vector> state;
            final TestP4Config conf = new TestP4Config();

            @Override
            public void processElement(InternalStream input, Context context, Collector<InternalStream> collector) throws Exception {
                CoProcessFunction.Context ctx = Mockito.mock(CoProcessFunction.Context.class);
                state.setEstimate(new Vector(generateSequence(5)));

                for (int subRounds = 0; subRounds < 50; subRounds++) {
                    // receiving Phi(Xi), new SubRound
                    InternalStream stream = new Zeta(1.0);
                    for (int i = 0; i < uniqueStreams; i++)
                        handleZeta(state, ((Zeta) stream).getPayload(), collector, conf);

                    assertFalse(state.getSync());

                    // receiving increment (violation), end of subRound
                    stream = new Increment(5);
                    handleIncrement(state, ((Increment) stream).getPayload(), collector, conf);
                    assertTrue(state.getSync());
                }
            }

            @Override
            public void open(Configuration parameters) {
                state = new CoordinatorStateHandler<>(getRuntimeContext(), conf);
            }
        }).addSink(new Testable.InternalStreamSink());
        env.execute();

        // TEST_CASE: assert that the broadcasted POJOs are the expected ones
        //for (int i = 0; i < Testable.InternalStreamSink.result.size(); i++)
            //System.out.println(Testable.InternalStreamSink.result.get(i).toString());
    }
}
