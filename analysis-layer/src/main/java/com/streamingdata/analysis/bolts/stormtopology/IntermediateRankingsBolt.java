package com.streamingdata.analysis.bolts.stormtopology;

import com.streamingdata.analysis.bolts.stormtopology.tools.Rankable;
import com.streamingdata.analysis.bolts.stormtopology.tools.RankableObject;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.*;
import java.util.stream.Collectors;

public final class IntermediateRankingsBolt extends BaseRichBolt {

    private static final long serialVersionUID = -8279088066553179058L;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private NavigableMap<RankableObject, Integer> ranking;
    private int maxRankingObjects;
    private final int emitFrequencyInSeconds;
    private OutputCollector collector;

    public IntermediateRankingsBolt(int maxRankingObjects, int emitFrequencyInSeconds) {
        if (maxRankingObjects < 1) {
            throw new IllegalArgumentException("Max ranking objects size should be more 0" );
        }
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException("Max ranking objects size should be more 0" );
        }

        this.maxRankingObjects = maxRankingObjects;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        ranking = new TreeMap<>();
    }

    public IntermediateRankingsBolt(int maxRankingObjects) {
        this(maxRankingObjects, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public IntermediateRankingsBolt() {
        this(DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
         this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
         if (TupleUtils.isTick(tuple)) {
             emitRanking();
         } else {
            addRanking(tuple);
         }
    }

    private void emitRanking() {
        List<Rankable> rankingCopy = ranking.keySet().stream().map(RankableObject::clone).
                collect(Collectors.toList());

        collector.emit(new Values(rankingCopy));
    }

    private void addRanking(Tuple tuple) {
        RankableObject rankingObject = createRankingObject(tuple);

        synchronized (ranking) {
            ranking.put(rankingObject, rankingObject.getCount());
            shrimpRankingToMaxSize();
        }

        collector.ack(tuple);
    }

    private void shrimpRankingToMaxSize() {
        while (ranking.size() > maxRankingObjects) {
            ranking.pollFirstEntry();
        }
    }

    private RankableObject createRankingObject(Tuple tuple) {
        Object obj = tuple.getValue(0);
        Integer count = tuple.getInteger(1);
        return new RankableObject(obj, count);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
         outputFieldsDeclarer.declare(new Fields("ranking"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
