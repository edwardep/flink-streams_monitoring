package test_utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Random;

public class Generators {

    public static HashMap<Tuple2<Integer, Integer>, Double> generateSequence(int range) {
        HashMap<Tuple2<Integer, Integer>, Double> map = new HashMap<>();
        for(int i = 0; i < range; i++)
            map.put(Tuple2.of(i,i), i*1.0);
        return map;
    }
    public static HashMap<Tuple2<Integer, Integer>, Double> generateSequence(int range, int scalar) {
        HashMap<Tuple2<Integer, Integer>, Double> map = new HashMap<>();
        for(int i = 0; i < range; i++)
            map.put(Tuple2.of(i,i), i*scalar*1.0);
        return map;
    }
    public static HashMap<Tuple2<Integer, Integer>, Double> generateSequence(int range, Double value) {
        HashMap<Tuple2<Integer, Integer>, Double> map = new HashMap<>();
        for(int i = 0; i < range; i++)
            map.put(Tuple2.of(i,i), value);
        return map;
    }
    public static Double[] uniform_random_vector(int n, double scale, double shift) {
        Random rand = new Random();
        Double[] res = new Double[n];
        for(int j=0; j < n; j++)
            res[j] =  (scale*rand.nextDouble())+shift;
        return res;
    }
}
