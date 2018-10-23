package com.streamingdata.analysis.bolts.stormtopology.tools;


import java.io.Serializable;

public interface Rankable extends Comparable, Serializable, Cloneable {
    Object getObject();

    Integer getCount();

    Rankable clone();
}
