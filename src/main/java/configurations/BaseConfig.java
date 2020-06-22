package configurations;

import com.esotericsoftware.kryo.NotNull;
import fgm.SafeZone;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;


public interface BaseConfig<AccType, VectorType, RecordType> extends Serializable {

    /**
     * This method is used internally in StateHandlers to initialize MapState with the appropriate Key type.<br>
     * Usage: "return Types.DOUBLE;"
     *
     * @return TypeInformation for Key in Flink's MapState
     */
    TypeInformation<VectorType> getVectorType();

    /**
     * Used internally in synchronization processes. This translates to 'k' in the fgm algorithm and it indicates<br>
     * the unique stream ids. The true parallelism will depend on Flink job parametrization.<br>
     * Usage: "return this.keyGroup.size();"
     *
     * @return the size of the keyGroup list
     */
    Integer uniqueStreams();

    /**
     * You can override this method in order to set the desired &psi; monitoring Quantization factor.<br>
     * By default it returns <b>0.01</b>
     * @return the quantization factor
     */
    default Double getMQF() { return 0.01; }

    VectorType newInstance();

    AccType newAccInstance();

    /**
     * Comments about VectorType: Either make the VectorType always return a non Null object
     * or handle null arguments in the methods below...otherwise NullPointerException is thrown
     */

    AccType aggregateRecord(RecordType record, AccType vector);

    AccType subtractAccumulators(AccType acc1, AccType acc2);

    VectorType updateVector(AccType accumulator, VectorType vector);

    VectorType addVectors(VectorType vector1, VectorType vector2);

    VectorType scaleVector(VectorType vector, Double scalar);

    double safeFunction(VectorType drift, VectorType estimate, SafeZone safeZone);

    String queryFunction(VectorType estimate, long timestamp);

    SafeZone initializeSafeZone(VectorType global);

    // compress

    // decompress
}
