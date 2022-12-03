package cn.fudan.cs.drpq.stree.data.arbitrary;

import cn.fudan.cs.drpq.stree.data.AbstractTreeNode;
import cn.fudan.cs.drpq.stree.util.Hasher;

import java.io.Serializable;
import java.util.LinkedList;

public class TreeNodeRAPQ<V> extends AbstractTreeNode<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> implements Serializable {

    private int hash = 0;

//    private SpanningTreeRAPQ tree;


    public TreeNodeRAPQ(V vertex, int state, LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        super(vertex, state, lower_bound_timestamp, upper_bound_timestamp);
    }

    public TreeNodeRAPQ(V vertex, int state, V attribute_root, long labeled_timestamp, LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        super(vertex, state, attribute_root, labeled_timestamp, lower_bound_timestamp, upper_bound_timestamp);
    }

//    @Override
//    public SpanningTreeRAPQ<V> getTree() {
//        return tree;
//    }


    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof TreeNodeRAPQ)) {
            return false;
        }

        TreeNodeRAPQ tuple = (TreeNodeRAPQ) o;

        return tuple.vertex.equals(vertex) && tuple.state == state;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if(h == 0) {
            if (attribute_root == null)
                h = Hasher.TreeNodeHasher(vertex.hashCode(), state, 0);
            else {
                h = 31 * h + vertex.hashCode();
                h = 31 * h + state;
                h = 31 * h + attribute_root.hashCode();
            }
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return "Node <" + getVertex() + "," + getState() + "," + getAttribute_root() + "> labeled ts = " + getLabeledTimestamp()
                + " [lower " + lower_bound_timestamp.toString()  + "] [upper " + upper_bound_timestamp.toString() + "]";
    }

}
