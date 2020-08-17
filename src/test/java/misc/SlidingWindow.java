package misc;

import configurations.BaseConfig;
import configurations.TestP1Config;
import datatypes.Accumulator;
import datatypes.InputRecord;
import datatypes.InternalStream;
import datatypes.Vector;
import operators.IncAggregation;
import operators.WindowFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;
import sources.SyntheticEventTimeSource;

import test_utils.Testable;

import java.io.IOException;
import java.io.Serializable;
import java.util.UUID;

import static junit.framework.TestCase.assertEquals;


public class SlidingWindow {


    public interface BaseConfigDeduper<In, Acc> extends Serializable {
        Acc createAcc();
        Acc addAccs(In in, Acc acc);
    }

    public static class ConfigDeduper implements BaseConfigDeduper<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAcc() {
            return new Tuple2<>("0", 0);
        }

        @Override
        public Tuple2<String, Integer> addAccs(Tuple2<String, Integer> in, Tuple2<String, Integer> acc) {
            acc.f1 += in.f1;
            return acc;
        }
    }

    public static class GenericAggregateFunctionDeduper<In, Acc> implements AggregateFunction<In, Acc, Acc> {

        BaseConfigDeduper<In, Acc> cfg;
        public GenericAggregateFunctionDeduper(BaseConfigDeduper<In, Acc> cfg) {
            this.cfg = cfg;
        }

        @Override
        public Acc createAccumulator() {
            return cfg.createAcc();
        }

        @Override
        public Acc add(In in, Acc acc) {
            return cfg.addAccs(in, acc);
        }

        @Override
        public Acc getResult(Acc acc) {
            return acc;
        }

        @Override
        public Acc merge(Acc acc, Acc acc1) {
            return null;
        }
    }


    @Test
    public void aggregateFunction_genericType() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String,Integer>> source = env.fromElements(Tuple2.of("0",1), Tuple2.of("0",2), Tuple2.of("0",3));

        source
                .keyBy(k -> k.f0)
                .countWindow(5, 1)
                .aggregate(new GenericAggregateFunctionDeduper<>(new ConfigDeduper()), Types.TUPLE(Types.STRING,Types.INT), Types.TUPLE(Types.STRING,Types.INT))
                .print();

        env.execute();
    }

    //todo: make IncAggregator abstract so the used have to implement it

    public interface MergedConfig<In, Acc, Out> extends BaseConfigAPI, AggregateFunction<In, Acc, Out> {}


    public static class NewConfigAPI implements MergedConfig<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("0", 0);
        }

        @Override
        public Tuple2<String, Integer> add(Tuple2<String, Integer> in, Tuple2<String, Integer> acc) {
            acc.f1 += in.f1;
            return acc;
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> acc) {
            return acc;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> acc1) {
            return null;
        }
    }


    public interface BaseConfigAPI extends Serializable {
        //These will be implemented directly from AggregateFunction
        //Acc createAcc();
        //Acc addAccumulators(In in, Acc acc);

        //other methods to be overridden
    }
//
//    public static class ConfigAPI implements BaseConfigAPI<Tuple2<String, Integer>, Tuple2<String,Integer>, Tuple2<String, Integer>> {
//        @Override
//        public Tuple2<String, Integer> createAcc() {
//            return new Tuple2<>("0", 0);
//        }
//
//        @Override
//        public Tuple2<String, Integer> addAccumulators(Tuple2<String, Integer> in, Tuple2<String, Integer> acc) {
//            acc.f1 += in.f1;
//            return acc;
//        }
//
//
//    }


//    public static class SimpleGenericAggregateFunc implements AggregateFunction< Tuple2< String, Integer >, Tuple2< String, Integer >, Tuple2< String, Integer > > {
//
//        private BaseConfigAPI<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> cfg;
//
//        SimpleGenericAggregateFunc(BaseConfigAPI<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> cfg) {
//            this.cfg = cfg;
//        }
//
//        @Override
//        public Tuple2<String, Integer> createAccumulator() {
//            return cfg.createAcc();
//        }
//
//        @Override
//        public Tuple2<String, Integer> add(Tuple2<String, Integer> in, Tuple2<String, Integer> acc) {
//            return cfg.addAccumulators(in, acc);
//        }
//
//        @Override
//        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> acc) {
//            return acc;
//        }
//
//        @Override
//        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
//            return a;
//        }
//    }

//    public static class GenericAggregateFunc<In, Acc, Out> implements AggregateFunction<In, Acc, Out> {
//
//        private BaseConfigAPI<In, Acc> cfg;
//        GenericAggregateFunc(BaseConfigAPI<In, Acc, Out> cfg) {
//            this.cfg = cfg;
//        }
//        @Override
//        public Acc createAccumulator() {
//            return cfg.createAcc();
//        }
//        @Override
//        public Acc add(In in, Acc acc) {
//            return cfg.addAccumulators(in, acc);
//        }
//        @Override
//        public Out getResult(Acc acc) {
//            return (Out)acc;
//        }
//        @Override
//        public Acc merge(Acc acc, Acc acc1) {
//            return null;
//        }
//    }


    @Test
    public void slidingWindow_vector_diff_test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        String defInputPath = "D:/Documents/WorldCup_tools/ita_public_tools/output/wc_day46_1.txt";
        int slide = 5;
        int window = 1000;

        TestP1Config cfg = new TestP1Config();

        KeyedStream<InputRecord, String> keyedStream = env
                .addSource(new SyntheticEventTimeSource())
                .map(x -> x)
                .returns(TypeInformation.of(InputRecord.class))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<InputRecord>() {
                    @Override
                    public long extractAscendingTimestamp(InputRecord inputRecord) {
                        return inputRecord.getTimestamp();
                    }
                })
                .keyBy(k->"0");

        DataStream<Accumulator<Vector>> windowedStream1 = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new IncAggregation<>(cfg), new WindowFunctionSimple<>());

        DataStream<Accumulator<Vector>> windowedStream2 = keyedStream
                .timeWindow(Time.seconds(window), Time.seconds(slide))
                .aggregate(new IncAggregation<>(cfg), new WindowFunction<>(cfg));


        windowedStream1
                .process(new ProcessFunction<Accumulator<Vector>, String>() {
                    @Override
                    public void processElement(Accumulator<Vector> input, Context context, Collector<String> collector) throws Exception {
                        collector.collect(cfg.queryFunction(input.getVec(), context.timestamp()));
                    }
                }).addSink(new Testable.WindowValidationSink1());

        windowedStream2
                .process(new ProcessFunction<Accumulator<Vector>, String>() {

                    Vector drift = new Vector();
                    @Override
                    public void processElement(Accumulator<Vector> input, Context context, Collector<String> collector) throws Exception {
                        Vector current = new Vector(input.getVec().map());
                        drift = cfg.addVectors(current, drift);
                        collector.collect(cfg.queryFunction(drift,context.timestamp()));
                    }
                }).addSink(new Testable.WindowValidationSink2());

        env.execute();

        int size1 = Testable.WindowValidationSink1.result1.size();
        int size2 = Testable.WindowValidationSink2.result2.size();
        assert size1 == size2;
        for(int i = 0; i < size1; i++)
            assertEquals("at"+i+" out of "+size1, Testable.WindowValidationSink1.result1.get(i),Testable.WindowValidationSink2.result2.get(i));
    }

    public static class WindowFunctionSimple<AccType> extends ProcessWindowFunction<Accumulator<AccType>, Accumulator<AccType>, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Accumulator<AccType>> iterable, Collector<Accumulator<AccType>> out) {
            if(iterable.iterator().hasNext()){
                out.collect(new Accumulator<>(key, iterable.iterator().next().getVec()));
            }
        }
    }

}
