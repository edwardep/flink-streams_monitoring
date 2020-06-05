package utils;

import org.apache.flink.api.common.state.MapState;

import java.util.HashMap;
import java.util.Map;

public class DoubleOperators {


    public static Double add(Double a, Double b) {
        if(a == null) a = 0d;
        if(b == null) b = 0d;
        return a + b;
    }

    public static Double subtract(Double a, Double b) {
        if(a == null) a = 0d;
        if(b == null) b = 0d;
        return a - b;
    }

    public static Double multiply(Double a, Double b) {
        if (a == null) a = 0d;
        if (b == null) b = 0d;
        return a * b;
    }

    public static Double divide(Double a, Double b) {
        if (a == null) a = 0d;
        if (b == null || b == 0d) throw new ArithmeticException("Division by 0 ~ Check your second argument.");
        return a / b;
    }


    public static <K> Map<K, Double> vec_add(Map<K, Double> A, Map<K, Double> B) {
        Map<K, Double> res = new HashMap<>(A);
        for(Map.Entry<K, Double> entry : B.entrySet())
            res.put(entry.getKey(), res.getOrDefault(entry.getKey(), 0d) + entry.getValue());
        return res;
    }

    public static <K> Map<K, Double> scaleVector(MapState<K, Double> vector, Double scalar) throws Exception {
        HashMap<K, Double> ret = new HashMap<>();
        for (Map.Entry<K, Double> entry : vector.entries())
            ret.put(entry.getKey(), scalar * entry.getValue());
        return ret;
    }

    public static <K> Iterable<Map.Entry<K, Double>> vec_add_map(MapState<K, Double> A,
                                                                 Map<K, Double> B) throws Exception {

        Map<K, Double> ret = new HashMap<>();
        for(Map.Entry<K, Double> entry : A.entries())
            ret.put(entry.getKey(), entry.getValue());

        for(Map.Entry<K, Double> entry : B.entrySet())
            ret.put(entry.getKey(), ret.getOrDefault(entry.getKey(), 0d) + entry.getValue());

        return ret.entrySet();
    }
    public static <K> Iterable<Map.Entry<K, Double>> vec_add_map(MapState<K, Double> A,
                                                                 MapState<K, Double> B) throws Exception {

        Map<K, Double> ret = new HashMap<>();
        for(Map.Entry<K, Double> entry : A.entries())
            ret.put(entry.getKey(), entry.getValue());

        for(Map.Entry<K, Double> entry : B.entries())
            ret.put(entry.getKey(), ret.getOrDefault(entry.getKey(), 0d) + entry.getValue());

        return ret.entrySet();
    }

    public static <K> Double norm(Iterable<Map.Entry<K, Double>> iterable) {
        Double res = 0d;
        for (Map.Entry<K, Double> entry : iterable)
            res = add(res, multiply(entry.getValue(), entry.getValue()));

        return Math.sqrt(res);
    }

    public static Double norm(Double[] vector) {
        double res = 0d;
        for (Double e : vector) res += e*e;
        return Math.sqrt(res);
    }

    public static <K> Map<K, Double> normalize(Iterable<Map.Entry<K, Double>> vector) {
        Map<K, Double> res = new HashMap<>();
        Double norm = norm(vector);
        try {
            for (Map.Entry<K, Double> entry : vector) {
                res.put(entry.getKey(), divide(entry.getValue(), norm));
            }
        } catch (ArithmeticException e) {
            e.printStackTrace();
        }
        return res;
    }

    public static <K> Map<K, Double> normalize(Iterable<Map.Entry<K, Double>> vector, double norm) {
        if(norm == 0)
            throw new ArithmeticException("norm value cannot be 0 when normalizing");
        Map<K, Double> res = new HashMap<>();
        for (Map.Entry<K, Double> entry : vector)
            res.put(entry.getKey(), entry.getValue()/norm);
        return res;
    }


    public static <K> Double dotProduct(Map<K, Double> A,
                                        Map<K, Double> B) {

        Double product = 0d;
        for (K key : A.keySet())
            product = add(product, multiply(A.get(key), B.getOrDefault(key, 0d)));
        return product;
    }

    public static <K> Double dotProductMap(Map<K, Double> A,
                                           Map<K, Double> B) {
        double product = 0d;
        for (K key : A.keySet()){
            if(B.containsKey(key))
                product += A.get(key) * B.get(key);
        }
        return product;
    }
    public static <K> Double dotProductMap(Map<K, Double> A,
                                           MapState<K, Double> B) throws Exception {
        double product = 0d;
        for (K key : A.keySet()){
            if(B.contains(key))
                product += A.get(key) * B.get(key);
        }
        return product;
    }
}
