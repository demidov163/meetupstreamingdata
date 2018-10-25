package com.streamingdata.analysis.bolts.stormtopology.tools;

public final class RankingObjectFactory {

    public static Rankable getRankingObject(Object obj, Integer count) {
         return new RankableObject(obj, count);
    }
}
