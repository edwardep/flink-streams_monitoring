package test_utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class Generators {

    public static HashMap<Tuple2<Integer, Integer>, Double> generateSequence(int range) {
        HashMap<Tuple2<Integer, Integer>, Double> map = new HashMap<>();
        for(int i = 0; i < range; i++)
            map.put(Tuple2.of(i,i), i*1.0);
        return map;
    }
    public static HashMap<Tuple2<Integer, Integer>, Double> generateSequence(int range, double scalar) {
        HashMap<Tuple2<Integer, Integer>, Double> map = new HashMap<>();
        for(int i = 0; i < range; i++)
            map.put(Tuple2.of(i,i), i*scalar);
        return map;
    }
}
