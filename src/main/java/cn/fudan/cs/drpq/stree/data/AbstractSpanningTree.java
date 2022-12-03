package cn.fudan.cs.drpq.stree.data;

import cn.fudan.cs.drpq.stree.data.arbitrary.SpanningTreeRAPQ;
import cn.fudan.cs.drpq.stree.util.Constants;
import cn.fudan.cs.drpq.stree.util.Hasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractSpanningTree<V, T extends AbstractSpanningTree<V, T, N>, N extends AbstractTreeNode<V, T, N>> {
    protected final Logger LOG = LoggerFactory.getLogger(SpanningTreeRAPQ.class);

    protected V root_vertex;
    protected N rootNode;
    protected Delta<V, T, N> delta;

    //一个key对应一个node的collection
    public ConcurrentHashMap<Hasher.MapKey<V>, N> nodeIndex;
//    public HashMultimap<V, N> vertexToNodes;
    /* 所有node的 ts 的最大值 */
    protected long maxTimestamp = 0;
    protected long minTimestamp = Long.MAX_VALUE;
    //expiry related data structures

    protected AbstractSpanningTree(Delta<V, T, N> delta, long minTimestamp, long maxTimestamp) {
//        this.minTimestamp = timestamp;
        this.nodeIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREE_SIZE, Constants.EXPECTED_LABELS);
//        this.vertexToNodes = HashMultimap.create();
        this.delta = delta;

        if (maxTimestamp != Long.MAX_VALUE)
            this.maxTimestamp = maxTimestamp;
        if (minTimestamp != 0)
            this.minTimestamp = minTimestamp;

//        candidates = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
//        candidateRemoval = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
//        visited = new HashSet<>(Constants.EXPECTED_TREE_SIZE);
    }

    public int getSize() {
        return nodeIndex.size();
    }

    /**
     * This function traverses down the entire tree and identify nodes with expired timestamp
     * Meanwhile, it finds the smallest timestamp valid timestamp to update the timestamp of tree
     * @param minTimestamp
     * @return min timestamp that is larger than the current min
     */

    //获取当前大于minTimestamp的最小时间，并且将小于minTimestamp的边全部放入candidates中
    protected abstract long populateCandidateRemovals(long minTimestamp);

    public N addNode(LinkedList<Long> parent_lower_list, LinkedList<Long> parent_upper_list, V childVertex, int childState, long timestamp, long windowSize) {
        N child = delta.getObjectFactory().createTreeNode((T) this, childVertex, childState);
        child.updateTimestamps(timestamp, windowSize, parent_lower_list, parent_upper_list);
        if (!child.upper_bound_timestamp.isEmpty()){
            nodeIndex.put(Hasher.createTreeNodePairKey(childVertex, childState), child);
            child.setAttribute_root(root_vertex);
            // a new node is added to the spanning tree. update delta index
            this.delta.addToTreeNodeIndex((T) this, child);
            this.delta.vertexStateToNodes.computeIfAbsent(Hasher.createTreeNodePairKey(childVertex, childState), k-> new HashSet<>());
            this.delta.vertexStateToNodes.get(Hasher.getThreadLocalTreeNodePairKey(childVertex, childState)).add(child);
//            this.updateMinTimestamp(timestamp);
//            this.updateMaxTimestamp(timestamp + windowSize);
            return child;
        }
        return null;
    }

    /**
     * removes old edges from the productGraph, used during window management.
     * This function assumes that expired edges are removed from the productGraph, so traversal assumes that it is guarenteed to
     * traverse valid edges
     * @param minTimestamp lower bound of the window interval. Any edge whose timestamp is smaller will be removed
     * @return The set of nodes that have expired from the window as there is no other path
     */
    public void removeOldEdges(long minTimestamp) {
        Collection<N> nodes = nodeIndex.values();
        for (N node : nodes) {
            if (node.upper_bound_timestamp.getLast() <= minTimestamp){
                nodeIndex.remove(Hasher.getThreadLocalTreeNodePairKey(node.getVertex(), node.getState()));
//                vertexToNodes.remove(node.getVertex(), node);
                delta.removeFromTreeIndex(node, (T) this);
            } else {
                while (node.upper_bound_timestamp.getFirst() < minTimestamp){
                    node.upper_bound_timestamp.removeFirst();
                    node.lower_bound_timestamp.removeFirst();
                }
            }
        }
        this.minTimestamp = minTimestamp;
    }

    public boolean exists(V vertex, int state) {
        return nodeIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    public N getNodes(V vertex, int state) {
        return nodeIndex.get(Hasher.getThreadLocalTreeNodePairKey(vertex, state));
    }

    public V getRootVertex() {
        return this.root_vertex;
    }

    public N getRootNode() {
        return this.rootNode;
    }

    public void updateTimestamp(long timestamp) {
        if(timestamp > maxTimestamp) {
            this.maxTimestamp = timestamp;
        } else if (timestamp < minTimestamp){
            this.minTimestamp = timestamp;
        }
    }
    public void updateMaxTimestamp(long timestamp) {
        if(timestamp > maxTimestamp) {
            this.maxTimestamp = timestamp;
        }
    }
    public void updateMinTimestamp(long timestamp) {
        if (timestamp < minTimestamp){
            this.minTimestamp = timestamp;
        }
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }
    public long getMinTimestamp() {
        return minTimestamp;
    }
    /**
     * Checks whether the entire three has expired, i.e. there is no active edge from the root node
     * @return <code>true</> if there is no active edge from the root node
     */
    public boolean isExpired(long minTimestamp) {
        return this.maxTimestamp <= minTimestamp;
    }

    @Override
    public String toString() {
        return "root : " + rootNode + " - labeled ts :" + rootNode.getLabeledTimestamp();
    }
}
