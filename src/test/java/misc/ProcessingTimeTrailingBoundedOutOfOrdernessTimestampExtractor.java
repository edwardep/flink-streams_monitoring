package misc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * This is a {@link AssignerWithPeriodicWatermarks} used to emit Watermarks that lag behind the
 * element with the maximum timestamp (in event time) seen so far by a fixed amount of time,
 * {@code t_late}. This can help reduce the number of elements that are ignored due to lateness when
 * computing the final result for a given window, in the case where we know that elements arrive no
 * later than {@code t_late} units of time after the watermark that signals that the system
 * event-time has advanced past their (event-time) timestamp.
 *
 * <p>In addition, this extractor will detect when there have not been any records for a given
 * time (the {@code idlenessDetectionDuration}. When the stream is considered idle this assigner
 * will emit a watermark that trails behind the current processing-time by
 * {@code processingTimeTrailingDuration}.
 */
public abstract class ProcessingTimeTrailingBoundedOutOfOrdernessTimestampExtractor<T>
        implements AssignerWithPeriodicWatermarks<T> {

    private static final long serialVersionUID = 1L;

    /**
     * The (fixed) interval between the maximum seen timestamp seen in the records
     * and that of the watermark to be emitted.
     */
    private final long maxOutOfOrderness;

    /**
     * After not extracting a timestamp for this duration the stream is considered idle. We will
     * then generate a watermark that trails by a certain amount behind processing time.
     */
    private final long idlenessDetectionDuration;

    /**
     * The amount by which the idle-watermark trails behind current processing time.
     */
    private final long processingTimeTrailingDuration;

    /** The current maximum timestamp seen so far. */
    private long currentMaxTimestamp;

    /** The timestamp of the last emitted watermark. */
    private long lastEmittedWatermark = Long.MIN_VALUE;

    private long lastUpdatedTimestamp = Long.MAX_VALUE;

    public ProcessingTimeTrailingBoundedOutOfOrdernessTimestampExtractor(
            Time maxOutOfOrderness,
            Time idlenessDetectionDuration,
            Time processingTimeTrailingDuration) {
        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the maximum out-of-orderness " +
                    "to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        }
        if (idlenessDetectionDuration.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the idleness detection duration " +
                    "to " + idlenessDetectionDuration + ". This parameter cannot be negative.");
        }
        if (processingTimeTrailingDuration.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the processing-time trailing duration " +
                    "to " + processingTimeTrailingDuration + ". This parameter cannot be negative.");
        }


        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        this.idlenessDetectionDuration = idlenessDetectionDuration.toMilliseconds();
        this.processingTimeTrailingDuration = processingTimeTrailingDuration.toMilliseconds();
        this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
    }

    /**
     * Extracts the timestamp from the given element.
     *
     * @param element The element that the timestamp is extracted from.
     * @return The new timestamp.
     */
    public abstract long extractTimestamp(T element);

    @Override
    public final Watermark getCurrentWatermark() {
        // also initialize the lastUpdatedTimestamp here in case we never saw an element
        if (lastUpdatedTimestamp == Long.MAX_VALUE) {
            lastUpdatedTimestamp = System.nanoTime();
        }

        // this guarantees that the watermark never goes backwards.
        long potentialWM = currentMaxTimestamp - maxOutOfOrderness;

        if (potentialWM > lastEmittedWatermark) {
            // update based on timestamps if we see progress
            lastEmittedWatermark = potentialWM;
        } else if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastUpdatedTimestamp) > idlenessDetectionDuration) {
            // emit a watermark based on processing time if we don't see elements for a while
            long processingTimeWatermark = System.currentTimeMillis() - processingTimeTrailingDuration;
            if (processingTimeWatermark > lastEmittedWatermark) {
                lastEmittedWatermark = processingTimeWatermark;
            }
        } else {
            System.out.println("DIFF " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastUpdatedTimestamp));
        }
        return new Watermark(lastEmittedWatermark);
    }

    @Override
    public final long extractTimestamp(T element, long previousElementTimestamp) {
        long timestamp = extractTimestamp(element);
        if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp;
        }
        lastUpdatedTimestamp = System.nanoTime();
        return timestamp;
    }
}
