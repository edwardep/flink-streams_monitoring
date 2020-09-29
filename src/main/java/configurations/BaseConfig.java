package configurations;

import datatypes.InternalStream;
import datatypes.internals.GlobalEstimate;
import fgm.SafeZone;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.Serializable;


public interface BaseConfig<VectorType> extends Serializable {

    /********************************************************
     *                STATE VECTOR SECTION                  *
     ********************************************************/

    /**
     * This method is used internally in StateHandlers to define the type of state vectors .<br>
     * Usage: "return TypeInformation.of(YourVectorType.class)"
     *
     * @return A type hint for value state creation
     */
    TypeInformation<VectorType> getVectorType();

    /**
     * Used internally by the kafka consumer when de-serializing the feedback messages.
     *
     * @return A jackson2 type reference for Object deserialization
     */
    TypeReference<GlobalEstimate<VectorType>> getTypeReference();

    /**
     * newVectorInstance() is called when initializing ValueState internally and whenever there is need for an empty Vector
     * @return a new Instance of Vector
     */
    VectorType newVectorInstance();

    /**
     * Pointwise Vector addition. Called by the coordinatorFunction when aggregating drift vectors or when updating
     * the global vector. It should return a new object and NOT alter the arguments.
     * @param vector1   vector1
     * @param vector2   vector2
     * @return  vector1 + vector2
     */
    VectorType addVectors(VectorType vector1, VectorType vector2);

    /**
     * Multiplying a vector with a scalar. It should return a new object and NOT alter the arguments.
     * @param vector    The vector
     * @param scalar    The scalar
     * @return  A new scaled vector
     */
    VectorType scaleVector(VectorType vector, Double scalar);

    /**
     *
     * @param inputRecord
     * @param vector
     * @return
     */
    VectorType updateVector(InternalStream inputRecord, VectorType vector);

    // NOT TESTED, just prototyping
    default <CompressedVector> CompressedVector compress(VectorType vector) { return (CompressedVector) vector; }
    default <CompressedVector> VectorType decompress(CompressedVector vector) { return (VectorType) vector; }


    /********************************************************
     *                       FGM SECTION                    *
     ********************************************************/

    /**
     * Used internally in synchronization processes. This translates to 'k' in the fgm algorithm and it indicates<br>
     * the number of workers (sites) <br>
     *
     * @return the number of sites
     */
    Integer workers();

    /**
     * You can override this method in order to set the desired monitoring Quantization factor.<br>
     * By default it returns <b>0.01</b>
     * @return the quantization factor
     */
    default Double getMQF() { return 0.01; }

    default Time warmup() { return Time.minutes(1); };

    default boolean rebalancingEnabled() { return false; }

    double safeFunction(VectorType drift, VectorType estimate, SafeZone safeZone);

    String queryFunction(VectorType estimate, long timestamp);

    SafeZone initializeSafeZone(VectorType global);

    /********************************************************
     *              SLIDING WINDOW SECTION                  *
     ********************************************************/
    default boolean slidingWindowEnabled() { return false; }

    default Time windowSize() { return Time.hours(1); }
    default Time windowSlide() { return Time.seconds(5); }
}
