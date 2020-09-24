package test_utils;

import datatypes.InternalStream;
import datatypes.internals.Input;
import org.apache.flink.api.java.tuple.Tuple2;
import sketches.AGMSSketch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class Generators {

    public static ArrayList<InternalStream> generateInputSequence(int range) {
        ArrayList<InternalStream> list = new ArrayList<>(range);
        for(int i = 0; i < range; i++)
            list.add(new Input("0",0L, Tuple2.of(i,i), i*1.0));
        return list;
    }

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
    public static double[] uniform_random_vector(int n, double scale, double shift) {
        Random rand = new Random();
        double[] res = new double[n];
        for(int j=0; j < n; j++)
            res[j] =  (scale*rand.nextDouble())+shift;
        return res;
    }

    public static AGMSSketch const_vector(int depth, int width, double value) {
        AGMSSketch res = new AGMSSketch(depth,width);
        for(int i = 0; i < depth; i++){
            for(int j = 0; j < width; j++)
                res.values()[i][j] = value;
        }
        return res;
    }

    public static double[][] const_vector2(int depth, int width, double value) {
        double[][] res = new double[depth][width];
        for(int i = 0; i < depth; i++){
            for(int j = 0; j < width; j++)
                res[i][j] = value;
        }
        return res;
    }

    public static double[] const_vector(int size, double value) {
        double[] res = new double[size];
        for(int i = 0; i < size; i++)
                res[i] = value;
        return res;
    }
}
