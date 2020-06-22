package utils;

import org.apache.flink.api.common.state.MapState;

import java.util.HashMap;
import java.util.Map;

public class DoubleOperators {

    public static <K> Map<K, Double> vec_add(Map<K, Double> A, Map<K, Double> B) {
        Map<K, Double> res = new HashMap<>(A);
        for(Map.Entry<K, Double> entry : B.entrySet())
            res.put(entry.getKey(), res.getOrDefault(entry.getKey(), 0d) + entry.getValue());
        return res;
    }

    public static <K> Double norm(Iterable<Map.Entry<K, Double>> iterable) {
        double res = 0d;
        for (Map.Entry<K, Double> entry : iterable)
            res += entry.getValue() * entry.getValue();
        return Math.sqrt(res);
    }

    public static <K> Map<K, Double> normalize(Iterable<Map.Entry<K, Double>> vector, double norm) {
        if(norm == 0)
            throw new ArithmeticException("norm value cannot be 0 when normalizing");
        Map<K, Double> res = new HashMap<>();
        for (Map.Entry<K, Double> entry : vector)
            res.put(entry.getKey(), entry.getValue()/norm);
        return res;
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
}
