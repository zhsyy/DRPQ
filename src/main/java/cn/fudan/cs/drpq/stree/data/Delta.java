package cn.fudan.cs.drpq.stree.data;

import cn.fudan.cs.drpq.stree.util.Constants;
import cn.fudan.cs.drpq.stree.util.Hasher;
import com.codahale.metrics.MetricRegistry;

import java.util.*;
import java.util.concurrent.*;

public class Delta<V, T extends AbstractSpanningTree<V, T, N>, N extends AbstractTreeNode<V, T, N>>{

    public ConcurrentHashMap<Hasher.MapKey_With_Root<V>, T> treeIndex;
    public ConcurrentHashMap<Hasher.MapKey<V>, Set<T>> nodeToTreeIndex;
    public ConcurrentHashMap<Hasher.MapKey<V>, Set<AbstractTreeNode>> vertexStateToNodes;

    private ObjectFactory<V, T, N> objectFactory;

//    private final Logger LOG = LoggerFactory.getLogger(Delta.class);

    public Delta(int capacity, ObjectFactory<V, T, N> objectFactory) {
        treeIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREES);
        nodeToTreeIndex = new ConcurrentHashMap<>(Constants.EXPECTED_TREES);
        vertexStateToNodes = new ConcurrentHashMap<>(Constants.EXPECTED_TREES);
        this.objectFactory = objectFactory;
    }

    public Collection<AbstractTreeNode>[] getAllNodes(HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> new_CWVs, int size,
                                                      long windowsize, ExecutorService executorService){
        List<Future<Collection<AbstractTreeNode>[]>> futures = new ArrayList<>(new_CWVs.size());
        CompletionService<Collection<AbstractTreeNode>[]> completionService = new ExecutorCompletionService<>(executorService);
        Collection<AbstractTreeNode>[] result_nodes = new ArrayList[size];

        for (int i = 1; i < result_nodes.length; i++) {
            result_nodes[i] = new ArrayList<>();
        }

        new_CWVs.forEach(((mapKey, longs) -> {
            FindCWVNodesJob<V> findCWVNodesJob = new FindCWVNodesJob(vertexStateToNodes, mapKey, longs, windowsize, size);
            futures.add(completionService.submit(findCWVNodesJob));
        }));

        for(int i = 0; i < futures.size(); i++) {
            try {
                Collection<AbstractTreeNode>[] return_nodes = completionService.take().get();
                for (int j = 1; j < size; j++) {
                    result_nodes[j].addAll(return_nodes[j]);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        return result_nodes;
    }

    public ObjectFactory<V, T, N> getObjectFactory() {
        return objectFactory;
    }

    public Collection<T> getTrees(V vertex, int state ) {
        Set<T> containingTrees = nodeToTreeIndex.computeIfAbsent(Hasher.createTreeNodePairKey(vertex, state), key -> Collections.newSetFromMap(new ConcurrentHashMap<T, Boolean>()) );
        return containingTrees;
    }

    public T getTreesByRoot(V vertex, int state, V root_attribute) {
        return treeIndex.get(Hasher.getThreadLocalTreeNodePairKey_With_Root(vertex, state, root_attribute));
    }

    public boolean exists(V vertex, int state, V root) {
        return treeIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey_With_Root(vertex, state, root));
    }

    public T addTree(V vertex, int state, V root_vertex, LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        T tree = objectFactory.createSpanningTree(this, vertex, state, root_vertex, lower_bound_timestamp, upper_bound_timestamp);
        treeIndex.put(Hasher.createTreeNodePairKeyWithRoot(vertex,state, root_vertex), tree);
        addToTreeNodeIndex(tree, tree.getRootNode());
        return tree;
    }

    public T addLocalTree(V vertex, long edge_time) {
        LinkedList<Long> upper = new LinkedList<>();
        upper.add(Long.MAX_VALUE);
        LinkedList<Long> lower = new LinkedList<>();
        lower.add((long) 0);
        T tree = objectFactory.createSpanningTree(this, vertex, 0, vertex, lower, upper);
        tree.minTimestamp = edge_time;
        treeIndex.put(Hasher.createTreeNodePairKeyWithRoot(vertex,0, vertex), tree);
        addToTreeNodeIndex(tree, tree.getRootNode());
        return tree;
    }

    public void removeTree(T tree) {
        N rootNode = tree.getRootNode();
        this.treeIndex.remove(Hasher.getThreadLocalTreeNodePairKey_With_Root(rootNode.getVertex(), rootNode.getState(), tree.root_vertex));
//        removeFromTreeIndex(rootNode, tree);
    }

    public void addToTreeNodeIndex(T tree, N treeNode) {
        Collection<T> containingTrees = getTrees(treeNode.getVertex(), treeNode.getState());
        containingTrees.add(tree);
    }

    public void removeFromTreeIndex(N removedNode, T tree) {
        Collection<T> containingTrees = this.getTrees(removedNode.getVertex(), removedNode.getState());
        containingTrees.remove(tree);
        if(containingTrees.isEmpty()) {
            this.nodeToTreeIndex.remove(Hasher.getThreadLocalTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
        }
        Set<AbstractTreeNode> nodes = vertexStateToNodes.get(Hasher.getThreadLocalTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
        if (nodes != null) {
            nodes.remove(removedNode);
            if (nodes.isEmpty())
                vertexStateToNodes.remove(Hasher.getThreadLocalTreeNodePairKey(removedNode.getVertex(), removedNode.getState()));
        }
    }


    /**
     * Updates Perform window expiry on each spanning tree
     * @param minTimestamp lower bound on the window interval
//     * @param productGraph snapshotGraph
     * @param executorService
     * @param <L>
     */
    public <L> void expiry(Long minTimestamp, ExecutorService executorService) {
        Collection<T> trees = treeIndex.values();
        List<Future<Void>> futures = new ArrayList<>(trees.size());
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

//        LOG.info("{} of trees in Delta", trees.size());
        for(T tree : trees) {
            if (tree.getMaxTimestamp() <= minTimestamp) {
                for (N node : tree.nodeIndex.values()) {
                    removeFromTreeIndex(node, tree);
                }
                removeTree(tree);
                continue;
            }
            if (tree.getMinTimestamp() >= minTimestamp)
                continue;

            RAPQSpanningTreeExpiryJob<V, L, T> RAPQSpanningTreeExpiryJob = new RAPQSpanningTreeExpiryJob(minTimestamp, tree);
            futures.add(completionService.submit(RAPQSpanningTreeExpiryJob));
        }

        for(int i = 0; i < futures.size(); i++) {
            try {
                completionService.take().get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
//                LOG.error("SpanningTreeExpiry interrupted during execution", e);
            }
        }

//        LOG.info("Expiry at {}: # of trees {}, # of edges in the productGraph {}", minTimestamp, treeIndex.size(), productGraph.getEdgeCount());
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
//        this.treeCounter = metricRegistry.counter("tree-counter");
//        this.treeSizeHistogram = metricRegistry.histogram("tree-size-histogram");
    }

    private static class RAPQSpanningTreeExpiryJob<V,L, T extends AbstractSpanningTree> implements Callable<Void> {

        private final long minTimestamp;
//        private ProductGraph<V,L> productGraph;

        private T tree;

        public RAPQSpanningTreeExpiryJob(Long minTimestamp, T tree) {
            this.minTimestamp = minTimestamp;
//            this.productGraph = productGraph;
            this.tree = tree;
        }

        @Override
        public Void call() throws Exception {
            tree.removeOldEdges(minTimestamp);
            return null;
        }
    }
    private static class FindCWVNodesJob<V> implements Callable<Collection<AbstractTreeNode>[]> {

        private final Hasher.MapKey<Integer> keys;
        private final Map<Integer, LinkedList<Long>> longs;
        private final long windowSize;
        private final int size;
        ConcurrentHashMap<Hasher.MapKey<V>, Set<AbstractTreeNode>> vertex_state2Nodes;

        public FindCWVNodesJob(ConcurrentHashMap<Hasher.MapKey<V>, Set<AbstractTreeNode>> vertex_state2Nodes, Hasher.MapKey<Integer> keys,
                               Map<Integer, LinkedList<Long>> longs, long windowSize, int size) {
            this.keys = keys;
            this.longs = longs;
            this.vertex_state2Nodes = vertex_state2Nodes;
            this.windowSize = windowSize;
            this.size = size;
        }

        @Override
        public Collection<AbstractTreeNode>[] call() {
            Collection<AbstractTreeNode>[] return_nodes = new ArrayList[size];
            for (int i = 1; i < size; i++) {
                return_nodes[i] = new ArrayList<>();
            }

            Collection<AbstractTreeNode> nodes = vertex_state2Nodes.get(keys);
//            if (keys.X == 1200 && keys.Y == 2){
//                System.out.println(MPI.COMM_WORLD.Rank() + " Delta " + nodes.toString());
//            }

            if (nodes != null) {
                longs.forEach((worker, ts_list) -> {
                    for (AbstractTreeNode node : nodes) {
//                        node.setLabeledTimestamp(ts_list.getFirst());
//                        node.CWV_on_worker.add(worker);
//                        return_nodes[worker].add(node);

                        if (!node.CWV_on_worker.contains(worker)) {
                            for (Long ts : ts_list) {
                                if (node.ifContainTs(ts, windowSize)) {
                                    node.setLabeledTimestamp(ts);
                                    node.CWV_on_worker.add(worker);
                                    return_nodes[worker].add(node);
                                    break;
                                }
                            }
                        }
                    }
                });
            }
            return return_nodes;
        }
    }
}
