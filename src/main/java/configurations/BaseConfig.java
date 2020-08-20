package configurations;

import datatypes.Vector;
import datatypes.internals.GlobalEstimate;
import fgm.SafeZone;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;

import java.io.Serializable;


public interface BaseConfig<AccType, VectorType, RecordType> extends Serializable {

    TypeInformation<AccType> getAccType();

    TypeReference<GlobalEstimate<VectorType>> getTypeReference();

    /**
     * This method is used internally in StateHandlers to define the type of state vectors .<br>
     * Usage: "return TypeInformation.of(YourVectorType.class)"
     *
     * @return A type hint for value state creation
     */
    TypeInformation<VectorType> getVectorType();

    /**
     * Used internally in synchronization processes. This translates to 'k' in the fgm algorithm and it indicates<br>
     * the number of workers (sites) <br>
     * Usage: "return this.keyGroup.size();"
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

    /**
     * newInstance() is called when initializing ValueState internally and whenever there is need for an empty Vector
     * @return a new Instance of Vector
     */
    VectorType newInstance();

    /**
     * This instance is used in the IncrementalWindowAggregation. It can be the same type as the Vector.
     * @return a new Instance of Accumulator
     */
    AccType newAccInstance();

    /**
     * This routine is called by the IncAggregation:add() method. It simply adds a new record to the Accumulator object.
     * Notice: It should not return a new Accumulator object but rather update the one provided as argument and return it.
     * @param record The incoming record object
     * @param vector The provided accumulator
     * @return  The updated accumulator
     */
    AccType aggregateRecord(RecordType record, AccType vector);

    /**
     * Called by WindowFunction on every slide. Because of the nature of the sliding window, a subtraction between the
     * current window and the previous one must be applied in order to extract the new and old values.
     * @param acc1 The current's window accumulator
     * @param acc2 The previous' window accumulator
     * @return  A new Accumulator Object with the pointwise subtraction of the two accumulators
     */
    AccType subtractAccumulators(AccType acc1, AccType acc2);

    /**
     * Called by WorkerFunction when updating the Drift vector. Iterate through the accumulator and update the provided
     * VectorType object, then return it.
     * @param accumulator   The accumulator containing the new and old values of the last window
     * @param vector    The drift vector
     * @return  The updated drift vector
     */
    VectorType updateVector(AccType accumulator, VectorType vector);

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

    double safeFunction(VectorType drift, VectorType estimate, SafeZone safeZone);

    String queryFunction(VectorType estimate, long timestamp);

    SafeZone initializeSafeZone(VectorType global);

    // compress

    // decompress
}
