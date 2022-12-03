package cn.fudan.cs.drpq.stree.engine;

import cn.fudan.cs.drpq.stree.data.arbitrary.SpanningTreeRAPQ;
import cn.fudan.cs.drpq.stree.data.arbitrary.TreeNodeRAPQ;
import cn.fudan.cs.drpq.stree.query.Automata;
import cn.fudan.cs.drpq.stree.util.Constants;
import cn.fudan.cs.drpq.stree.util.Hasher;
import cn.fudan.cs.drpq.stree.data.*;

import java.util.*;

public class TreeNodeRAPQTreeExpansionJob<L> extends AbstractTreeExpansionJob<L, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>{
//    static boolean debug =false;
    private HashSet<AbstractTreeNode>[] prepar_send_nodes;

    public TreeNodeRAPQTreeExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results,
                                        long windowsize, int size, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWV_table) {
        super(productGraph, automata, results, windowsize);
        this.size = size;
//        this.CWVs = CWVs;
        // initialize node types
        this.CWV_table = CWV_table;
        this.spanningTree = new SpanningTreeRAPQ[Constants.EXPECTED_BATCH_SIZE];
        this.parentNode = new TreeNodeRAPQ[Constants.EXPECTED_BATCH_SIZE];
        prepar_send_nodes = new HashSet[size];
    }

    @Override
    public Collection<AbstractTreeNode>[] call() throws Exception {
        // call each job in teh buffer

        for (int i = 1; i < size; i++) {
            prepar_send_nodes[i] = new HashSet<>();
        }

        for (int i = 0; i < currentSize; i++) {
            processTransition(spanningTree[i], parentNode[i], targetVertex[i], targetState[i], edgeTimestamp[i], labeledTimestamp[i]);
        }
        return prepar_send_nodes;
    }

    public void processTransition(SpanningTreeRAPQ<Integer> tree, TreeNodeRAPQ<Integer> parentNode,
                                                          int childVertex, int childState, long edgeTimestamp, long labeledTimestamp) {
        LinkedList<LinkedList<Long>> parentNode_upper_ts = new LinkedList<>();
        parentNode_upper_ts.add(parentNode.upper_bound_timestamp);
        LinkedList<LinkedList<Long>> parentNode_lower_ts = new LinkedList<>();
        parentNode_lower_ts.add(parentNode.lower_bound_timestamp);
        LinkedList<Integer> childVertexList = new LinkedList<>();
        childVertexList.add(childVertex);
        LinkedList<Integer> childStateList = new LinkedList<>();
        childStateList.add(childState);
        LinkedList<Long> edgeTimestampList = new LinkedList<>();
        edgeTimestampList.add(edgeTimestamp);

        while (!childVertexList.isEmpty()){
            LinkedList<Long> upper_ts = parentNode_upper_ts.removeFirst();
            LinkedList<Long> lower_ts = parentNode_lower_ts.removeFirst();
            boolean modify = false;
            TreeNodeRAPQ<Integer> childNode;
            childVertex = childVertexList.removeFirst();
            childState = childStateList.removeFirst();
            edgeTimestamp = edgeTimestampList.removeFirst();

            if(tree.exists(childVertex, childState)) {
                // if the child node already exists, we might need to update timestamp
                childNode = tree.getNodes(childVertex, childState);
                modify = childNode.updateTimestamps(edgeTimestamp, windowSize, lower_ts, upper_ts);
                childNode.setLabeledTimestamp(labeledTimestamp);
            } else {
                childNode = tree.addNode(lower_ts, upper_ts, childVertex, childState, edgeTimestamp, windowSize);
                if (childNode != null) {
                    childNode.setLabeledTimestamp(labeledTimestamp);
                    modify = true;
                    if (CWV_table.containsKey(Hasher.getThreadLocalTreeNodePairKey(childVertex, childState))){
                        Map<Integer, LinkedList<Long>> ts_list = CWV_table.get(Hasher.getThreadLocalTreeNodePairKey(childVertex, childState));
//                        long finalEdgeTimestamp = edgeTimestamp;
//                        ts_list.forEach((id, list) -> {
////                            if (list.getLast() + windowSize > finalEdgeTimestamp) {
//                                childNode.CWV_on_worker.add(id);
////                            }
//                        });

                        ts_list.forEach((worker, list) -> {
                            if (!childNode.CWV_on_worker.contains(worker)) {
                                for (Long ts : list) {
                                    if (childNode.ifContainTs(ts, windowSize)) {
                                        childNode.CWV_on_worker.add(worker);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                }
            }

            if (modify) {
                tree.updateMaxTimestamp(childNode.upper_bound_timestamp.getLast());

                if (automata.isFinalState(childState) && tree.getRootVertex() != childVertex) {
                    ResultPair<Integer> resultPair = new ResultPair<>(tree.getRootVertex(), childVertex, System.currentTimeMillis(),
                            (LinkedList<Long>) childNode.lower_bound_timestamp.clone(), (LinkedList<Long>) childNode.upper_bound_timestamp.clone());
//                    if (resultPair.getSource() == 372 && resultPair.getTarget() == 7566) {
//                        System.out.println("Edge Get result : " + System.currentTimeMillis() + " " + tree.toString());
//                        System.out.println(" node index : "+ tree.nodeIndex.toString());
//                    }
                    results.add(resultPair);
                    resultCount++;
                }

//                for (int i = 1; i < size; i++) {
//                    if (childNode.CWV_on_worker.contains(i)){
//                        prepar_send_nodes[i].add(childNode);
//                    } else {
//                        if (CWV_table.containsKey(Hasher.getThreadLocalTreeNodePairKey(childVertex, childState))){
//                            Map<Integer, LinkedList<Long>> ts_list = CWV_table.get(Hasher.getThreadLocalTreeNodePairKey(childVertex, childState));
//                            if (ts_list.containsKey(i)){
//                                for (Long aLong : ts_list.get(i)) {
//                                    if (childNode.ifContainTs(aLong, windowSize)){
//                                        childNode.setLabeledTimestamp(aLong);
//                                        childNode.CWV_on_worker.add(i);
//                                        prepar_send_nodes[i].add(childNode);
//                                        break;
//                                    }
//                                }
//                            }
//                        }
//                    }
//                }
//                if (childVertex == 372 && childState == 2 && childNode.getAttribute_root() == 372 && childNode.CWV_on_worker.contains(1)) {
//                    System.out.println(MPI.COMM_WORLD.Rank() + " -> " + childNode + " send on processing labeled time = " + labeledTimestamp);
//                }
                childNode.CWV_on_worker.forEach(integer -> {
                    if (!prepar_send_nodes[integer].contains(childNode))
                        prepar_send_nodes[integer].add(childNode);
                });

                Collection<GraphEdge<ProductGraphNode<Integer>>> forwardEdges = productGraph.getForwardEdges(childVertex, childState);
                if (forwardEdges != null) {
                    // there are forward edges, iterate over them
                    for (GraphEdge<ProductGraphNode<Integer>> forwardEdge : forwardEdges) {
                        if (forwardEdge.getTimestamp() + windowSize <= childNode.lower_bound_timestamp.getFirst() ||
                                forwardEdge.getTimestamp() >= childNode.upper_bound_timestamp.getLast())
                            continue;
                        // recursive call as the target of the forwardEdge has not been visited in state targetState before
                        childVertexList.add(forwardEdge.getTarget().getVertex());
                        childStateList.add(forwardEdge.getTarget().getState());
                        edgeTimestampList.add(forwardEdge.getTimestamp());
                        parentNode_lower_ts.add(childNode.lower_bound_timestamp);
                        parentNode_upper_ts.add(childNode.upper_bound_timestamp);
                    }
                }
            }
        }

//        if (parentNode.getVertex() == 3043 && childVertex == 26165)
//            System.out.println("end tree: " + tree);
    }
}
