package datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
        return vector.getOrDefault(key, 0d);
    }

    public boolean isEmpty() {
        return vector.isEmpty();
    }

    public String toString() {
        return vector.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Vector vector1 = (Vector) o;
        return Objects.equals(vector, vector1.vector);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vector);
    }
}
