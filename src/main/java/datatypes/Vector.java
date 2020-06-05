package datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Vector implements Serializable {
    private Map<Tuple2<Integer, Integer>, Double> vector;

    public Vector() {
        this.vector = new HashMap<>();
    }
    public Vector(Map<Tuple2<Integer,Integer>, Double> vec) {
        this.vector = new HashMap<>(vec);
    }

    public Map<Tuple2<Integer, Integer>, Double> map() {
        return vector;
    }

    public Double getValue(Tuple2<Integer, Integer> key) {
        return vector.get(key) == null ? 0d : vector.get(key);
    }

    public boolean isEmpty() {
        return vector == null;
    }

    public String toString() {
        return vector.toString();
    }

}
