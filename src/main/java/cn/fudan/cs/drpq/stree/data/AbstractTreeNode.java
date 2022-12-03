package cn.fudan.cs.drpq.stree.data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

//一个node只包含一个parent，但是可以有很多的children
public abstract class AbstractTreeNode<V, T extends AbstractSpanningTree<V, T, N>, N extends AbstractTreeNode<V, T, N>> implements Serializable {

    protected V vertex;
    protected int state;
    protected long labeledTimestamp;
    public LinkedList<Long> lower_bound_timestamp;
    public LinkedList<Long> upper_bound_timestamp;
    protected V attribute_root;
    public boolean ifAlreadySend;
    public Set<Integer> CWV_on_worker;

    protected AbstractTreeNode(V vertex, int state, LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        this.vertex = vertex;
        this.state = state;
        this.lower_bound_timestamp = lower_bound_timestamp;
        this.upper_bound_timestamp = upper_bound_timestamp;
        CWV_on_worker = new HashSet<>();
    }

    protected AbstractTreeNode(V vertex, int state, V attribute_root, long labeled_timestamp, LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        this.vertex = vertex;
        this.state = state;
        this.attribute_root =  attribute_root;
        this.labeledTimestamp = labeled_timestamp;

        this.lower_bound_timestamp = lower_bound_timestamp;
        this.upper_bound_timestamp = upper_bound_timestamp;
        CWV_on_worker = new HashSet<>();
    }

    public boolean ifContainTs(long ts, long windowsize){
        Iterator<Long> lower_iterater = this.lower_bound_timestamp.iterator();
        Iterator<Long> upper_iterater = this.upper_bound_timestamp.iterator();

        while (lower_iterater.hasNext()){
            if (lower_iterater.next() < ts + windowsize && upper_iterater.next() > ts)
                return true;
        }
        return false;
    }

    public boolean updateTimestamps(long timestamp, long windowSize, LinkedList<Long> parent_lower_list, LinkedList<Long> parent_upper_list){
        /* edge's time interval [lower, upper) */
        LinkedList<Long> lower = new LinkedList<>();
        LinkedList<Long> upper = new LinkedList<>();
        long lower_ts = timestamp;
        long upper_ts = timestamp + windowSize;

        Iterator<Long> input_lower_iterator = parent_lower_list.iterator();
        Iterator<Long> input_upper_iterator = parent_upper_list.iterator();

        while (input_lower_iterator.hasNext()){
            long low = input_lower_iterator.next();
            long upp = input_upper_iterator.next();
            //有交集
            if (upp > lower_ts && low < upper_ts ){
                lower.add(Math.max(low, lower_ts));
                upper.add(Math.min(upp, upper_ts));
            } else if (low > upper_ts) {
                break;
            }
        }
        if (lower.isEmpty())
            return false;

        return updateTimestamps(lower, upper);
    }

    // 这个方法会修改输入的对象
    public boolean updateTimestamps(LinkedList<Long> lower_list, LinkedList<Long> upper_list){
        boolean change = false;
        if (lower_bound_timestamp.size() == 0){
            lower_bound_timestamp.addAll(lower_list);
            upper_bound_timestamp.addAll(upper_list);
            change = true;
        } else {
            int size = lower_list.size();
            for (int i = 0; i < size; i++) {
                long lower = lower_list.removeFirst();
                long upper = upper_list.removeFirst();

                if (!upper_bound_timestamp.isEmpty() && upper_bound_timestamp.getLast() < lower) {
                    lower_bound_timestamp.add(lower);
                    upper_bound_timestamp.add(upper);
                    lower_bound_timestamp.addAll(lower_list);
                    upper_bound_timestamp.addAll(upper_list);
                    change = true;
                    break;
                }
                Iterator<Long> source_lower_iterator = lower_bound_timestamp.iterator();
                Iterator<Long> source_upper_iterator = upper_bound_timestamp.iterator();

                if (source_lower_iterator.hasNext() && lower_bound_timestamp.getFirst() > lower){
                    while (upper_bound_timestamp.size() > 0 && upper_bound_timestamp.getFirst() <= upper){
                        upper_bound_timestamp.removeFirst();
                        lower_bound_timestamp.removeFirst();
                    }
                    if (lower_bound_timestamp.size() > 0 && lower_bound_timestamp.getFirst() <= upper){
                        lower_bound_timestamp.removeFirst();
                        lower_bound_timestamp.addFirst(lower);
                    } else {
                        lower_bound_timestamp.addFirst(lower);
                        upper_bound_timestamp.addFirst(upper);
                    }
                    change = true;
                } else {
                    int index_u = 0;
                    int index_l = 0;
                    boolean insert_lower = false, insert_upper = false;
                    while (source_lower_iterator.hasNext()){
                        long low = source_lower_iterator.next();
                        long upp = source_upper_iterator.next();

                        //有交集
                        index_u++;
                        index_l++;
                        if (upp >= lower && low <= upper){
                            if (upper > upp) {
                                source_upper_iterator.remove();
                                index_u--;
                                change = true;
                            }
                            else {
                                // 不插入upper
                                insert_upper = true;
                            }
                            if (low > lower){
                                source_lower_iterator.remove();
                                index_l--;
                                change = true;
                            } else {
                                // 不插入lower
                                insert_lower = true;
                            }
                        }
                        else if (low > upper) {
                            index_u--;
                            index_l--;
                            break;
                        }
                    }
                    if (!insert_lower) {
                        lower_bound_timestamp.add(index_l, lower);
                        change = true;
                    }
                    if (!insert_upper) {
                        upper_bound_timestamp.add(index_u, upper);
                        change = true;
                    }
                }
            }
        }
        return change;
    }

    public boolean updateTimestampsAndAddWhenNeeded(LinkedList<Long> lower_list, LinkedList<Long> upper_list){
        boolean change = false;
        if (lower_bound_timestamp.size() == 0){
            lower_bound_timestamp.addAll(lower_list);
            upper_bound_timestamp.addAll(upper_list);
            change = true;
        } else {
            int size = lower_list.size();
            for (int i = 0; i < size; i++) {
                long lower = lower_list.removeFirst();
                long upper = upper_list.removeFirst();

                if (upper_bound_timestamp.getLast() < lower) {
                    lower_bound_timestamp.add(lower);
                    upper_bound_timestamp.add(upper);
                    lower_bound_timestamp.addAll(lower_list);
                    upper_bound_timestamp.addAll(upper_list);
                    change = true;
                    break;
                }
                Iterator<Long> source_lower_iterator = lower_bound_timestamp.iterator();
                Iterator<Long> source_upper_iterator = upper_bound_timestamp.iterator();

                if (source_lower_iterator.hasNext() && lower_bound_timestamp.getFirst() > lower){
                    while (upper_bound_timestamp.size() > 0 && upper_bound_timestamp.getFirst() <= upper){
                        upper_bound_timestamp.removeFirst();
                        lower_bound_timestamp.removeFirst();
                    }
                    if (lower_bound_timestamp.size() > 0 && lower_bound_timestamp.getFirst() <= upper){
                        lower_bound_timestamp.removeFirst();
                        lower_bound_timestamp.addFirst(lower);
                    } else {
                        lower_bound_timestamp.addFirst(lower);
                        upper_bound_timestamp.addFirst(upper);
                    }
                    change = true;
                } else {
                    int index_u = 0;
                    int index_l = 0;
                    boolean insert_lower = false, insert_upper = false;
                    while (source_lower_iterator.hasNext()){
                        long low = source_lower_iterator.next();
                        long upp = source_upper_iterator.next();

                        //有交集
                        index_u++;
                        index_l++;
                        if (upp >= lower && low <= upper){
                            if (upper > upp) {
                                source_upper_iterator.remove();
                                index_u--;
                                change = true;
                            }
                            else {
                                // 不插入upper
                                insert_upper = true;
                            }
                            if (low > lower){
                                source_lower_iterator.remove();
                                index_l--;
                                change = true;
                            } else {
                                // 不插入lower
                                insert_lower = true;
                            }
                        }
                        else if (low > upper) {
                            index_u--;
                            index_l--;
                            break;
                        }
                    }
                    if (!insert_lower) {
                        lower_bound_timestamp.add(index_l, lower);
                        change = true;
                    }
                    if (!insert_upper) {
                        upper_bound_timestamp.add(index_u, upper);
                        change = true;
                    }
                }
            }
        }
        return change;
    }

//    public abstract AbstractSpanningTree<V, T, N> getTree();

    public V getVertex() {
        return vertex;
    }

    public int getState() {
        return state;
    }

    public long getLabeledTimestamp() {
        return labeledTimestamp;
    }

    public void setLabeledTimestamp(long labeledTimestamp) {
        this.labeledTimestamp = labeledTimestamp;
//        this.getTree().updateTimestamp(timestamp);
    }

    @Override
    public int hashCode() {
        int h = 17;
        h = 31 * h + vertex.hashCode();
        h = 31 * h + state;
        h = 31 * h + attribute_root.hashCode();
//        h = 31 * h + (int) labeledTimestamp;
        return h;
    }

    /**
     * Sets the timestamp of this node to Long.MIN for expiry ro remove after an explicit deletion
     * it does not set the timestamp for the tree
     */
    public void setDeleted() {
        this.labeledTimestamp = Long.MIN_VALUE;
    }

    public V getAttribute_root() {
        return attribute_root;
    }

    public void setAttribute_root(V attribute_root) {
        this.attribute_root = attribute_root;
    }
}
