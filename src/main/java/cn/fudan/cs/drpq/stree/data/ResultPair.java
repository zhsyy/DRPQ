package cn.fudan.cs.drpq.stree.data;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

public class ResultPair<V> implements Serializable, Comparable {

    private int hash = 0;

    private final V source;
    private final V target;
//    private boolean isDeletion;
    private final long clock_ts;
//    private final long first_label_ts;
    private final LinkedList<Long> lower_ts;
    private final LinkedList<Long> upper_ts;
//    private final long label_ts;

    public ResultPair(final V source, final V target, long clock_ts, LinkedList<Long> lower_ts, LinkedList<Long> upper_ts) {
        this.source = source;
        this.target = target;
        this.clock_ts = clock_ts;
        this.lower_ts = lower_ts;
        this.upper_ts = upper_ts;
//        first_label_ts = lower_ts.getFirst();
    }

    public V getSource() {
        return source;
    }

    public V getTarget() {
        return target;
    }

    public long getClock_ts() {
        return clock_ts;
    }

    public LinkedList<Long> getLower_ts() {
        return lower_ts;
    }

    public LinkedList<Long> getUpper_ts() {
        return upper_ts;
    }

    public boolean contain(long label_ts) {
        Iterator<Long> iterator_low =  lower_ts.iterator();
        Iterator<Long> iterator_upp =  upper_ts.iterator();

        while (iterator_low.hasNext()){
            if (iterator_low.next() < label_ts && iterator_upp.next() >= label_ts)
                return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = 17;
            h = 31 * h + source.hashCode();
            h = 31 * h + target.hashCode();
            h = 31 * h + lower_ts.hashCode();
            h = 31 * h + upper_ts.hashCode();
            h = 31 * h + (int) clock_ts;
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return "ResultPair{" +
                "source=" + source +
                ", target=" + target +
                ", clock_ts=" + clock_ts +
                ", lower=" + lower_ts.toString() +
                ", upper=" + upper_ts.toString() +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
