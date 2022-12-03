package cn.fudan.cs.drpq.stree.engine;

import cn.fudan.cs.drpq.stree.data.arbitrary.SpanningTreeRAPQ;
import cn.fudan.cs.drpq.stree.data.arbitrary.TreeNodeRAPQ;
import cn.fudan.cs.drpq.stree.query.Automata;
import cn.fudan.cs.drpq.stree.util.Hasher;
import cn.fudan.cs.drpq.stree.data.*;

import java.util.*;

public class CWVNodesExpansionJob<L> extends AbstractTreeExpansionJob<L, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>> {
//    SpanningTreeRAPQ<Integer> tree;
//    Set<ResultPair<Integer>> results;
//    ProductGraph<Integer, String> productGraph;
//    long windowsize;
//    Automata<String> automata;
//    Set<Hasher.MapKey<Integer>> CWVs;
    private HashSet<AbstractTreeNode>[] prepar_send_nodes;
    public CWVNodesExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results,
                                long windowsize, SpanningTreeRAPQ<Integer> tree, int size, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWV_table) {
        super(productGraph, automata, results, windowsize, tree);
        this.CWV_table = CWV_table;
        this.size = size;
//        this.CWVs = CWVs;
        prepar_send_nodes = new HashSet[size];
    }

    @Override
    public Collection<AbstractTreeNode>[] call() {
        Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(tree.getRootNode().getVertex(), tree.getRootNode().getState());
        for (int i = 1; i < size; i++) {
            prepar_send_nodes[i] = new HashSet<>();
        }
        if (forwardEdges == null) {
            // end recursion if node has no forward edges
            return prepar_send_nodes;
        } else {
            // there are forward edges, iterate over them
            for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                if (forwardEdge.getTimestamp() + windowSize > tree.getMinTimestamp() && forwardEdge.getTimestamp() < tree.getMaxTimestamp()) {
                    processCWVNode(forwardEdge.getTarget().getVertex(), forwardEdge.getTarget().getState(), forwardEdge.getTimestamp());
                }
            }
        }
        return prepar_send_nodes;
    }

    public void processCWVNode(int child_vertex, int child_state, long edge_time) {
        LinkedList<LinkedList<Long>> parentNode_upper_ts = new LinkedList<>();
        parentNode_upper_ts.add(tree.getRootNode().upper_bound_timestamp);
        LinkedList<LinkedList<Long>> parentNode_lower_ts = new LinkedList<>();
        parentNode_lower_ts.add(tree.getRootNode().lower_bound_timestamp);
        LinkedList<Integer> childVertexList = new LinkedList<>();
        childVertexList.add(child_vertex);
        LinkedList<Integer> childStateList = new LinkedList<>();
        childStateList.add(child_state);
        LinkedList<Long> edgeTimestampList = new LinkedList<>();
        edgeTimestampList.add(edge_time);

//        long metric_begin = System.currentTimeMillis();

        LinkedHashSet<TreeNodeRAPQ<Integer>> new_added_node = new LinkedHashSet<>();

        while (!childVertexList.isEmpty()){
            LinkedList<Long> upper_ts = parentNode_upper_ts.removeFirst();
            LinkedList<Long> lower_ts = parentNode_lower_ts.removeFirst();

            boolean modify = false;
            TreeNodeRAPQ<Integer> childNode;
            int childVertex = childVertexList.removeFirst();
            int childState = childStateList.removeFirst();
            long edgeTimestamp = edgeTimestampList.removeFirst();

            if (tree.exists(childVertex, childState)) {
                // if the child node already exists, we might need to update timestamp
                childNode = tree.getNodes(childVertex, childState);
                modify = childNode.updateTimestamps(edgeTimestamp, windowSize, lower_ts, upper_ts);
                childNode.setLabeledTimestamp(tree.getRootNode().getLabeledTimestamp());
            } else {
                childNode = tree.addNode(lower_ts, upper_ts, childVertex, childState, edgeTimestamp, windowSize);
                if (childNode != null) {
                    childNode.setLabeledTimestamp(tree.getRootNode().getLabeledTimestamp());
                    modify = true;
                    new_added_node.add(childNode);
                }
            }

            if (modify) {
                if (automata.isFinalState(childState) && tree.getRootVertex() != childVertex) {
                    ResultPair<Integer> resultPair = new ResultPair<>(tree.getRootVertex(), childVertex, System.currentTimeMillis(),
                            (LinkedList<Long>) childNode.lower_bound_timestamp.clone(), (LinkedList<Long>) childNode.upper_bound_timestamp.clone());
                    results.add(resultPair);
                    resultCount++;
                }

                childNode.CWV_on_worker.forEach(integer -> {
                    if (!prepar_send_nodes[integer].contains(childNode))
                        prepar_send_nodes[integer].add(childNode);
                });

                Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);
                if (forwardEdges != null) {
                    // there are forward edges, iterate over them
                    for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                        // recursive call as the target of the forwardEdge has not been visited in state targetState before
                        if (forwardEdge.getTimestamp() + windowSize <= childNode.lower_bound_timestamp.getFirst() ||
                                forwardEdge.getTimestamp() >= childNode.upper_bound_timestamp.getLast())
                            continue;

                        childVertexList.add(forwardEdge.getTarget().getVertex());
                        childStateList.add(forwardEdge.getTarget().getState());
                        edgeTimestampList.add(forwardEdge.getTimestamp());
                        parentNode_lower_ts.add(childNode.lower_bound_timestamp);
                        parentNode_upper_ts.add(childNode.upper_bound_timestamp);
                    }
                }
            }
        }

        for (TreeNodeRAPQ<Integer> integerTreeNodeRAPQ : new_added_node) {
            if (CWV_table.containsKey(Hasher.getThreadLocalTreeNodePairKey(integerTreeNodeRAPQ.getVertex(), integerTreeNodeRAPQ.getState()))){
                Map<Integer, LinkedList<Long>> ts_list = CWV_table.get(Hasher.getThreadLocalTreeNodePairKey(integerTreeNodeRAPQ.getVertex(), integerTreeNodeRAPQ.getState()));
                ts_list.forEach((worker, list) -> {
                    if (!integerTreeNodeRAPQ.CWV_on_worker.contains(worker)) {
                        for (Long ts : list) {
                            if (integerTreeNodeRAPQ.ifContainTs(ts, windowSize)) {
                                integerTreeNodeRAPQ.CWV_on_worker.add(worker);
                                if (!prepar_send_nodes[worker].contains(integerTreeNodeRAPQ))
                                    prepar_send_nodes[worker].add(integerTreeNodeRAPQ);
                                break;
                            }
                        }
                    }
                });
            }
        }
    }
}
