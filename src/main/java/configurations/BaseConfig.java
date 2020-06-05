package configurations;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;
import java.util.List;


public interface BaseConfig<VectorType, RecordType> extends Serializable {

    /**
     * This method is used internally in StateHandlers to initialize MapState with the appropriate Key type.<br>
     * Usage: "return Types.DOUBLE;"
     *
     * @return TypeInformation for Key in Flink's MapState
     */
    TypeInformation<VectorType> getVectorType();

    /**
     * Flink-fgm module uses internally this list of unique stream IDs to correctly broadcast the feedback to the<br>
     * distributed worker nodes. You could initialize the list in the constructor and simply handle this as a Getter.<br>
     * Usage: "return this.keyGroup;"
     *
     * @return java.List of stream IDs
     */
    List<String> getKeyGroup();

    /**
     * Used internally in synchronization processes. This translates to 'k' in the fgm algorithm and it indicates<br>
     * the distributed parallelism of the system. The true parallelism will depend on Flink job parametrization.<br>
     * Usage: "return this.keyGroup.size();"
     *
     * @return the size of the keyGroup list
     */
    Integer getKeyGroupSize();

    /**
     * You can override this method in order to set the desired &psi; monitoring Quantization factor.<br>
     * By default it returns <b>0.01</b>
     * @return the quantization factor
     */
    default Double getMQF() { return 0.01; }

    VectorType addVectors(VectorType vector1, VectorType vector2);

    VectorType subtractVectors(VectorType vector1, VectorType vector2);

    VectorType scaleVector(VectorType vector, Double scalar);

    double safeFunction(VectorType drift, VectorType estimate);

    String queryFunction(VectorType estimate, long timestamp);

    VectorType batchUpdate(Iterable<RecordType> iterable);
    // compress

    // decompress
}
