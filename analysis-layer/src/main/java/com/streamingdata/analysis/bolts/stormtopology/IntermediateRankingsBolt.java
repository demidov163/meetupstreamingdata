package com.streamingdata.analysis.bolts.stormtopology;

import com.streamingdata.analysis.bolts.stormtopology.tools.Rankable;
import com.streamingdata.analysis.bolts.stormtopology.tools.RankingObjectFactory;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public final class IntermediateRankingsBolt extends AbstractRankingsBolt {

    private static final long serialVersionUID = -8279088066553179058L;

    public IntermediateRankingsBolt(int maxRankingObjects, int emitFrequencyInSeconds) {
        super(maxRankingObjects, emitFrequencyInSeconds);
    }

    public IntermediateRankingsBolt(int maxRankingObjects) {
        super(maxRankingObjects);
    }

    public IntermediateRankingsBolt() {
        super();
    }

    protected void addRanking(Tuple tuple) {
        Rankable rankingObject = RankingObjectFactory.getRankingObject(tuple.getValue(0),
                tuple.getInteger(1));

        //sync on ranking map, need to prevent concurrent access in sequence of actions.
        synchronized (ranking) {
            ranking.add(rankingObject);
            shrinkRankingToMaxSize();
        }

        collector.ack(tuple);
        System.out.println("IntermediateRankingsBolt. Tuple to ranking");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
         outputFieldsDeclarer.declare(new Fields("ranking"));
    }
}
