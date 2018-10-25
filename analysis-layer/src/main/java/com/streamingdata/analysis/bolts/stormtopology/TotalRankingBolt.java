package com.streamingdata.analysis.bolts.stormtopology;

import com.streamingdata.analysis.bolts.stormtopology.tools.Rankable;
import org.apache.storm.tuple.Tuple;

import java.util.List;

public final class TotalRankingBolt extends AbstractRankingsBolt {

    @Override
    protected void addRanking(Tuple tuple) {
        //get entities from tuple
        List<Rankable> listOfRanking = (List<Rankable>) tuple.getValue(0);

        //add enteties to ranking set
        synchronized (ranking)  {
            ranking.addAll(listOfRanking);
            shrinkRankingToMaxSize();
        }

        //ack
        collector.ack(tuple);

        System.out.println("TotalRankingBolt. Tuple is ack");
    }
}
