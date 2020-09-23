package operators;

import datatypes.InternalStream;
import datatypes.internals.Input;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomSlidingWindow extends KeyedProcessFunction<String, InternalStream, InternalStream> {

    private final long windowSize;
    private final long windowSlide;

    public CustomSlidingWindow(Time size, Time slide){
        windowSize = size.toMilliseconds();
        windowSlide = slide.toMilliseconds();
    }

    private transient MapState<Long, List<InternalStream>> state;
    private transient ValueState<Long> currentSlideTimestamp;

    @Override
    public void processElement(InternalStream internalStream, Context context, Collector<InternalStream> collector) throws Exception {
        if(currentSlideTimestamp.value() == null) currentSlideTimestamp.update(0L);
        long currentEventTimestamp = ((Input)internalStream).getTimestamp();

        // Assign new SlideTimestamp if currentEventTimestamp has exceeded this Slide's time span
        // and collect all corresponding events
        if(currentEventTimestamp - currentSlideTimestamp.value() >= windowSlide) {
            collectPreviousSlideEvents(collector);
            currentSlideTimestamp.update(currentEventTimestamp);
            registerCleanupTimer(context);
        }
        // Add event to state
        addToState(internalStream);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<InternalStream> out) throws Exception {
        evictOutOfScopeElementsFromWindow(timestamp, out);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "window-state",
                Types.LONG, Types.LIST(Types.POJO(InternalStream.class))));

        currentSlideTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "currentSlide",
                Types.LONG));
    }

    private void registerCleanupTimer(Context context) throws IOException {
        // Register timers to ensure state cleanup
        long cleanupTime = currentSlideTimestamp.value() + windowSize - windowSlide;
        context.timerService().registerEventTimeTimer(cleanupTime);
    }

    private void evictOutOfScopeElementsFromWindow(Long threshold, Collector<InternalStream> out){
        try {
            Iterator<Long> keys = state.keys().iterator();
            while (keys.hasNext()) {
                Long stateEventTime = keys.next();
                if (stateEventTime + windowSize < threshold) {
                    collectEvictedEvents(stateEventTime, out);
                    keys.remove();
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    private void collectPreviousSlideEvents(Collector<InternalStream> out) throws IOException {
        Long key = currentSlideTimestamp.value();
        try {
            if(state.get(key) != null) {
                for (InternalStream event : state.get(key))
                    out.collect(event);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void collectEvictedEvents(Long key, Collector<InternalStream> out) {
        try {
            for(InternalStream event : state.get(key)){
                ((Input)event).setTimestamp(key);
                ((Input)event).setVal(-1.0);
                out.collect(event);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void addToState(InternalStream value) throws Exception {
        Long key = currentSlideTimestamp.value();
        List<InternalStream> currentSlide = state.get(key);
        if (currentSlide == null)
            currentSlide = new ArrayList<>();
        ((Input)value).setTimestamp(key); // assign the events the timestamp of the slide
        currentSlide.add(value);
        state.put(key, currentSlide);
    }
}
