package com.streamingdata.analysis.bolts.stormtopology.tools;


public class RankableObject implements Rankable {

    private static final long serialVersionUID = -3265139155107612314L;

    private final Object object;
    private final Integer count;

    public RankableObject(Object object, Integer count) {
        if (object == null || count == null) {
            throw new IllegalArgumentException("Object or Count is null");
        }
        this.object = object;
        this.count = count;
    }

    @Override
    public Object getObject() {
        return object;
    }

    @Override
    public Integer getCount() {
        return count;
    }

    @Override
    public int compareTo(Object anotherObject) {
        if (anotherObject instanceof Rankable) {
            return count - ((Rankable) anotherObject).getCount();
        }  else {
            throw new IllegalArgumentException("Need to compare Rankable objects.");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RankableObject that = (RankableObject) o;

        return object.equals(that.getObject());
    }

    @Override
    public int hashCode() {
        int result = object.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }

    @Override
    public Rankable clone() {
        return new RankableObject(object, count);
    }
}
