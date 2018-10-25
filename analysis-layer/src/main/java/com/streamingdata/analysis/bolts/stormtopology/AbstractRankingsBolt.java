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

public abstract class AbstractRankingsBolt extends BaseRichBolt {

    private static final long serialVersionUID = -8279088066553179058L;
    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    protected final NavigableSet<Rankable> ranking;
    protected int maxRankingObjects;
    protected final int emitFrequencyInSeconds;
    protected OutputCollector collector;

    protected AbstractRankingsBolt(int maxRankingObjects, int emitFrequencyInSeconds) {
        if (maxRankingObjects < 1) {
            throw new IllegalArgumentException("Max ranking objects size should be more 0" );
        }
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException("Max ranking objects size should be more 0" );
        }

        this.maxRankingObjects = maxRankingObjects;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        ranking = new TreeSet<>();
    }

    protected AbstractRankingsBolt(int maxRankingObjects) {
        this(maxRankingObjects, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    protected AbstractRankingsBolt() {
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
        List<Rankable> rankingCopy = ranking.stream().map(Rankable::clone).
                collect(Collectors.toCollection(ArrayList::new));
        //set desc order in collection
        Collections.reverse(rankingCopy);
        collector.emit(new Values(rankingCopy));
        //todo log
        System.out.println("Emit emitRanking.");
    }

    protected abstract void addRanking(Tuple tuple);

    protected void shrinkRankingToMaxSize() {
        while (ranking.size() > maxRankingObjects) {
            //because asc order of element
            ranking.pollFirst();
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
