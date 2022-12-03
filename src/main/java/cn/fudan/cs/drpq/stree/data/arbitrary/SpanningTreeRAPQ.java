package cn.fudan.cs.drpq.stree.data.arbitrary;

import cn.fudan.cs.drpq.stree.data.AbstractSpanningTree;
import cn.fudan.cs.drpq.stree.data.Delta;
import cn.fudan.cs.drpq.stree.util.Hasher;

import java.util.LinkedList;

public class SpanningTreeRAPQ<V> extends AbstractSpanningTree<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> {

    protected SpanningTreeRAPQ(Delta<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> delta, V vertex, int state, V root,
                               LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        super(delta, lower_bound_timestamp.getFirst(), upper_bound_timestamp.getLast());

        /* 这个点一定是root点 */
        this.rootNode = new TreeNodeRAPQ<V>(vertex, state, (LinkedList<Long>) lower_bound_timestamp.clone(), (LinkedList<Long>) upper_bound_timestamp.clone());
        this.root_vertex = root;
        this.rootNode.setAttribute_root(root);
        nodeIndex.put(Hasher.createTreeNodePairKey(vertex, state), rootNode);
    }

    @Override
    protected long populateCandidateRemovals(long minTimestamp) {
        // perform a bfs traversal on tree, no need for visited as it is a three
        LinkedList<TreeNodeRAPQ<V>> queue = new LinkedList<>();

        // minTimestamp of the tree should be updated, find the lowest timestamp in the tree higher than the minTimestmap
        // because after this maintenance, there is not going to be a node in the tree lower than the minTimestamp
        long minimumValidTimetamp = Long.MAX_VALUE;
//        queue.addAll(rootNode.getChildren());
//        //使用queue一层一层地遍历整棵树
//        while(!queue.isEmpty()) {
//            // populate the queue with children
//            TreeNodeRAPQ<V> currentVertex = queue.remove();
//            queue.addAll(currentVertex.getChildren());
//
//            // check time timestamp to decide whether it is expired
//            if(currentVertex.getTimestamp() <= minTimestamp) {
//                candidates.add(currentVertex);
//            }
//            // find minValidTimestamp for filtering for the next maintenance window
//            //找到下一次迭代时最小的timestamp
//            if(currentVertex.getTimestamp() > minTimestamp && currentVertex.getTimestamp() < minimumValidTimetamp) {
//                minimumValidTimetamp = currentVertex.getTimestamp();
//            }
//        }

        return minimumValidTimetamp;
    }

}
