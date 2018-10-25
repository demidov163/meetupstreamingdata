package com.streamingdata.analysis.bolts.stormtopology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public final class RollingObjectCountAggBolt extends BaseRichBolt {

    private static final long serialVersionUID = 3879075919179311L;

    private OutputCollector collector;
    private Map<Object, Map<Integer, Long>> counts = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Object groupName = tuple.getValue(0);
        Long count = tuple.getLong(1);
        int sourceTask = tuple.getSourceTask();
        long sum = counts.compute(groupName,
            (k, v) -> {
                if (v == null) {
                    Map<Integer, Long> subCount = new HashMap<>();
                    subCount.put(sourceTask, count);
                    return subCount;
                } else {
                    v.put(sourceTask, count);
                }
                return v;
            }
        ).values().stream().reduce(Long::sum).orElse(0L);

        collector.emit(new Values(groupName, sum));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
         declarer.declare(new Fields("obj", "count"));
    }
}
