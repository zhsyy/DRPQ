package cn.fudan.cs.drpq.stree.engine;

import cn.fudan.cs.drpq.input.InputTuple;
import cn.fudan.cs.drpq.stree.data.AbstractSpanningTree;
import cn.fudan.cs.drpq.stree.data.AbstractTreeNode;
import cn.fudan.cs.drpq.stree.data.Delta;
import cn.fudan.cs.drpq.stree.data.ObjectFactory;
import cn.fudan.cs.drpq.stree.data.arbitrary.ObjectFactoryArbitrary;
import cn.fudan.cs.drpq.stree.query.Automata;
import cn.fudan.cs.drpq.stree.util.Constants;
import cn.fudan.cs.drpq.stree.util.Hasher;
import cn.fudan.cs.drpq.stree.util.Semantics;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.concurrent.*;

public class WindowedRPQ<L, T extends AbstractSpanningTree<Integer, T, N>, N extends AbstractTreeNode<Integer, T, N>> extends RPQEngine<L> {

    private long windowSize;
    private long slideSize;
    private long lastExpiry = 0;
    private Semantics semantics;

    // runs all pairs reachability be default
    private boolean allPairs;
    public int TRD_count;

    public Delta<Integer, T, N> delta;
    ObjectFactory<Integer, T, N> objectFactory;


    private ExecutorService executorService;

    private int numOfThreads;

    private int received_count = 0;


    public void printAllTree(){
        for (AbstractSpanningTree tree : delta.treeIndex.values()) {
            System.out.println(tree + " <" + tree.getMinTimestamp() + ", " + tree.getMaxTimestamp()+">");
            System.out.println("    nodeIndex : " + tree.nodeIndex.toString());
        }
    }

//    private final Logger LOG = LoggerFactory.getLogger(WindowedRPQ.class);

    /**
     * Windowed RPQ engine ready to process edges
     * @param query Automata representation of the persistent query
     * @param capacity Initial size for internal data structures. Set to approximate number of edges in a window
     * @param windowSize Size of the sliding window in milliseconds
     * @param slideSize Slide interval in milliseconds
     * @param numOfThreads Total number of executor threads
     * @param semantics Resulting path semantics: @{@link Semantics}
     */
    public WindowedRPQ(Automata<L> query, int capacity, long windowSize, long slideSize, int numOfThreads, Semantics semantics, int size, int rank) {
        super(query, capacity, size);
        if (semantics.equals(Semantics.ARBITRARY)) {
            this.objectFactory = new ObjectFactoryArbitrary();
        } else {
//            this.objectFactory = new ObjectFactorySimple();
        }
        this.delta =  new Delta<Integer, T, N>(capacity, objectFactory);
        this.windowSize = windowSize;
        this.slideSize = slideSize;
        this.executorService = Executors.newFixedThreadPool(numOfThreads);
        this.numOfThreads = numOfThreads;
        this.semantics = semantics;

        // all pair RPQ processing by default
        this.allPairs = true;
        this.rank = rank;
    }


    @Override
    public void addMetricRegistry(MetricRegistry metricRegistry) {
        this.delta.addMetricRegistry(metricRegistry);
        // call super function to include all other histograms
        super.addMetricRegistry(metricRegistry);
    }


    @Override
    public Collection<AbstractTreeNode>[] processEdge(InputTuple<Integer, Integer, L> inputTuple, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWVs_latest) {
        received_count++;

        // total number of trees expanded due this edge insertion
        HashSet<AbstractTreeNode>[] prepar_sending_nodes = new HashSet[size];
        for (int i = 1; i < size; i++) {
            prepar_sending_nodes[i] = new HashSet<>();
        }
//        int treeCount = 0;
        //for now window processing is done inside edge processing
        long currentTimestamp = inputTuple.getTimestamp();
        //lastExpiry是当前窗口中最后一个过期的边的timestamp

        Map<Integer, Integer> transitions = automata.getTransition(inputTuple.getLabel());

        if(transitions.isEmpty()) {
            // there is no transition with given label, simply return
            return prepar_sending_nodes;
        } else {
            // add edge to the snapshot productGraph
            productGraph.addEdge(inputTuple.getSource(), inputTuple.getTarget(), inputTuple.getLabel(), inputTuple.getTimestamp());
            edgeCount++;
        }

        // edge is an insertion
        //create a spanning tree for the source node in case it does not exists

        if (transitions.containsKey(0) && !delta.exists(inputTuple.getSource(), 0, inputTuple.getSource())) {
            // if there exists a start transition with given label, there should be a spanning tree rooted at source vertex
            delta.addLocalTree(inputTuple.getSource(), currentTimestamp);
        }

        List<Future<Collection<AbstractTreeNode>[]>> futureList = new ArrayList<>();
        CompletionService<Collection<AbstractTreeNode>[]> completionService = new ExecutorCompletionService<>(this.executorService);

        List<Map.Entry<Integer, Integer>> transitionList = new ArrayList<>(transitions.entrySet());
        AbstractTreeExpansionJob treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, windowSize, size, CWVs_latest);

//        long unic_id = System.nanoTime();

        // for each transition that given label satisfy
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
            int targetState = transition.getValue();

            if (!delta.nodeToTreeIndex.containsKey(Hasher.getThreadLocalTreeNodePairKey(inputTuple.getSource(), sourceState)))
                continue;
            Collection<T> containingTrees = delta.getTrees(inputTuple.getSource(), sourceState);

            boolean runParallel = containingTrees.size() > Constants.EXPECTED_BATCH_SIZE * this.numOfThreads;


            for (T spanningTree : containingTrees) {
                // source is guarenteed to exists due to above loop,
                // we do not check target here as even if it exist, we might update its timestamp

                if (spanningTree.getRootNode().getState() != 0 && spanningTree.getMaxTimestamp() != 0 && spanningTree.getMaxTimestamp() < currentTimestamp){
                    continue;
                }

                N parentNode = spanningTree.getNodes(inputTuple.getSource(), sourceState);

                treeExpansionJob.addJob(spanningTree, parentNode, inputTuple.getTarget(), targetState, inputTuple.getTimestamp(), inputTuple.getTimestamp());
                // check whether the job is full and ready to submit
                if (treeExpansionJob.isFull()) {
                        if (runParallel) {
                            futureList.add(completionService.submit(treeExpansionJob));
                            treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, windowSize, size, CWVs_latest);
                        } else {
                            try {
                                Collection<AbstractTreeNode>[] return_nodes = treeExpansionJob.call();
                                for (int j = 1; j < size; j++) {
                                    prepar_sending_nodes[j].addAll(return_nodes[j]);
                                }
                                treeExpansionJob = objectFactory.createExpansionJob(productGraph, automata, results, windowSize, size, CWVs_latest);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
//                    }
                }
            }


            // wait for results of al jobs before moving to next transition to ensure that there is a single thread working on a tree
            for (int i = 0; i < futureList.size(); i++) {
                try {
                    Collection<AbstractTreeNode>[] return_nodes = completionService.take().get();

                    for (int j = 1; j < size; j++) {
                        prepar_sending_nodes[j].addAll(return_nodes[j]);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    System.out.println("error: index " + i + " / size " + futureList.size());
                    e.printStackTrace();
                }
            }
            futureList = new ArrayList<>();
            // if there is any remaining job in the buffer, run them in main thread
            if (!treeExpansionJob.isEmpty()) {
                try {
                    Collection<AbstractTreeNode>[] return_nodes = treeExpansionJob.call();
                    for (int j = 1; j < size; j++) {
                        prepar_sending_nodes[j].addAll(return_nodes[j]);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // metric recording
//        Long edgeElapsedTime = System.nanoTime() - edgeStartTime;

        return prepar_sending_nodes;
    }

    @Override
    public Collection<AbstractTreeNode>[] processNodes(AbstractTreeNode[] nodes, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWVs_latest) {
        HashSet<AbstractTreeNode>[] prepar_sending_nodes = new HashSet[size];
        for (int i = 1; i < size; i++) {
            prepar_sending_nodes[i] = new HashSet<>();
        }
        List<Future<Collection<AbstractTreeNode>[]>> futureList = Lists.newArrayList();
        CompletionService<Collection<AbstractTreeNode>[]> completionService = new ExecutorCompletionService<>(this.executorService);

        HashMap<Hasher.MapKey_With_Root<Integer>, AbstractTreeNode<Integer, T, N>> node_set = new HashMap<>();

        /* merge the time interval of the same node in advance */
        for (int j = 0; j < nodes.length; j++) {
            AbstractTreeNode<Integer, T, N> node = nodes[j];
            if (node_set.containsKey(Hasher.getThreadLocalTreeNodePairKey_With_Root(node.getVertex(), node.getState(), node.getAttribute_root()))){
                AbstractTreeNode<Integer, T, N> exists_node = node_set.get(Hasher.getThreadLocalTreeNodePairKey_With_Root(node.getVertex(), node.getState(), node.getAttribute_root()));
                exists_node.updateTimestamps(node.lower_bound_timestamp, node.upper_bound_timestamp);
                /* 取最小的满足条件的ts作为label ts */
            } else {
                node_set.put(Hasher.createTreeNodePairKeyWithRoot(node.getVertex(), node.getState(), node.getAttribute_root()), node);
            }
        }

        boolean runParallel = node_set.size() > this.numOfThreads;
        for (AbstractTreeNode<Integer, T, N> node : node_set.values()) {
            if (delta.exists(node.getVertex(), node.getState(), node.getAttribute_root())){
                T tree = delta.getTreesByRoot(node.getVertex(), node.getState(), node.getAttribute_root());
                tree.getRootNode().setLabeledTimestamp(node.getLabeledTimestamp());

                boolean modify;
                modify = tree.getRootNode().updateTimestamps(node.lower_bound_timestamp, node.upper_bound_timestamp);
                if (modify){
                    tree.setMaxTimestamp(tree.getRootNode().upper_bound_timestamp.getLast());
                    tree.setMinTimestamp(tree.getRootNode().lower_bound_timestamp.getFirst());
                    AbstractTreeExpansionJob treeExpansionJob = objectFactory.createCWVExpansionJob(productGraph, automata, results, windowSize, tree, size, CWVs_latest);
                    if (runParallel) {
                        futureList.add(completionService.submit(treeExpansionJob));
                    } else {
                        try {
                            Collection<AbstractTreeNode>[] return_nodes = treeExpansionJob.call();
                            for (int j = 1; j < size; j++) {
                                prepar_sending_nodes[j].addAll(return_nodes[j]);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            } else {
                T tree = delta.addTree(node.getVertex(), node.getState(), node.getAttribute_root(), node.lower_bound_timestamp, node.upper_bound_timestamp);
                tree.getRootNode().setLabeledTimestamp(node.getLabeledTimestamp());
                AbstractTreeExpansionJob treeExpansionJob = objectFactory.createCWVExpansionJob(productGraph, automata, results, windowSize, tree, size, CWVs_latest);
                if (runParallel) {
                    futureList.add(completionService.submit(treeExpansionJob));
                } else {
                    try {
                        Collection<AbstractTreeNode>[] return_nodes = treeExpansionJob.call();
                        for (int j = 1; j < size; j++) {
                            prepar_sending_nodes[j].addAll(return_nodes[j]);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        for (int i = 0; i < futureList.size(); i++) {
            try {
                Future<Collection<AbstractTreeNode>[]> future = completionService.poll(10, TimeUnit.SECONDS);
                if (future == null){
                    System.out.println("error: index " + i + " / size " + futureList.size() + " node.size " + nodes.length);
                } else {
                    Collection<AbstractTreeNode>[] return_nodes = future.get();
                    for (int j = 1; j < size; j++) {
                        prepar_sending_nodes[j].addAll(return_nodes[j]);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                System.out.println("error in node: index " + i + " / size " + futureList.size());
                e.printStackTrace();
            }
        }

        return prepar_sending_nodes;
    }

    @Override
    public Collection<AbstractTreeNode>[] getNodes(HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> new_CWVs){
        return delta.getAllNodes(new_CWVs, size, windowSize, this.executorService);
    }

    @Override

    public void shutDown() {
        // shutdown executors
        this.executorService.shutdown();
    }

    /**
     * updates Delta and Spanning Trees and removes any node that is lower than the window endpoint
     * might need to traverse the entire spanning tree to make sure that there does not exists an alternative path
     */
    @Override
    public void expiry(long minTimestamp) {
//        LOG.info("Expiry procedure at timestamp: {}", minTimestamp);
        // first remove the expired edges from the productGraph
        productGraph.removeOldEdges(minTimestamp);
        // then maintain the spanning trees, not that spanning trees are maintained without knowing which edge is deleted
        delta.expiry(minTimestamp, this.executorService);
    }

}
