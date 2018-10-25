package com.streamingdata.analysis.bolts.stormtopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.Map;

@Deprecated
public final class RollingObjectCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 7287392308497261309L;
    private static final int NUM_WINDOW_CHUNKS = 5;
    private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
            "Actual window length is %d seconds when it should be %d seconds"
                    + " (you can safely ignore this warning during the startup phase)";

    private final int windowLengthInSeconds;
    private final int emitFrequencyInSeconds;
    //private SlidingWindowCounter counter;
    private OutputCollector collector;
   // private NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingObjectCountBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        this.windowLengthInSeconds = windowLengthInSeconds;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
       // counter = new SlidingWindowCounter<>(deriveNumWindowChunksFrom(windowLengthInSeconds, emitFrequencyInSeconds));
    }

    private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int emitFrequencyInSeconds) {
        return windowLengthInSeconds / emitFrequencyInSeconds;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
       // lastModifiedTracker = new NthLastModifiedTimeTracker(
        //        deriveNumWindowChunksFrom(this.windowLengthInSeconds, this.emitFrequencyInSeconds));
    }

    @Override
    public void execute(Tuple tuple) {
         if (TupleUtils.isTick(tuple)) {
             System.out.println("Received tick tuple, triggering emit of current window counts");
             emitCurrentWindowCounts();
         } else {
             countGroupAndAck(tuple);
         }

    }

    private void countGroupAndAck(Tuple tuple) {
        Object obj = tuple.getValue(0);
        //counter.incrementCount(obj);
        collector.ack(tuple);
    }
    private void emitCurrentWindowCounts() {
       // Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
       // int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
       // lastModifiedTracker.markAsModified();
        //if (actualWindowLengthInSeconds != windowLengthInSeconds) {
       //     System.out.println(String.format(WINDOW_LENGTH_WARNING_TEMPLATE,
       //             actualWindowLengthInSeconds, windowLengthInSeconds));
       // }
       // emit(counts, actualWindowLengthInSeconds);
    }

    private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {
        counts.entrySet().forEach((Map.Entry<Object, Long> entry)->
             collector.emit(new Values(entry.getKey(), entry.getValue(), actualWindowLengthInSeconds)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
         outputFieldsDeclarer.declare(new Fields("groupname", "count", "actualwindowlengthinseconds"));
    }
}
