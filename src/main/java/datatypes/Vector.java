package datatypes;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Vector extends HashMap<Tuple2<Integer,Integer>, Double> {
    public Vector(){
        super();
    }
    public Vector(Vector vector) {
        super(vector);
    }
    public Vector(HashMap<Tuple2<Integer,Integer>, Double> map){
        super(map);
    }
}
