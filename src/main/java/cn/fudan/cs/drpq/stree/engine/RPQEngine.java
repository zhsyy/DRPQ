package cn.fudan.cs.drpq.stree.engine;

import cn.fudan.cs.drpq.input.InputTuple;
import cn.fudan.cs.drpq.stree.data.AbstractTreeNode;
import cn.fudan.cs.drpq.stree.data.ProductGraph;
import cn.fudan.cs.drpq.stree.data.ResultPair;
import cn.fudan.cs.drpq.stree.query.Automata;
import cn.fudan.cs.drpq.stree.util.Hasher;
import com.codahale.metrics.*;
import com.codahale.metrics.Timer;

import java.util.*;
import java.util.concurrent.TimeUnit;

public abstract class RPQEngine<L> {

    protected MetricRegistry metricRegistry;
    protected Counter resultCounter;
    protected Histogram containingTreeHistogram;
    protected Histogram fullHistogram;
    protected Histogram processedHistogram;
    protected Histogram explicitDeletionHistogram;
    protected Histogram fullProcessedHistogram;
    protected Histogram windowManagementHistogram;
    protected Histogram edgeCountHistogram;
    protected Timer fullTimer;
    protected ProductGraph<Integer, L> productGraph;
    protected Automata<L> automata;
    protected int size;
    protected int rank;

    protected Set<ResultPair<Integer>> results;

    protected int edgeCount = 0;


    protected RPQEngine(Automata<L> query, int capacity, int size) {
        automata = query;
//        results = new HashSet<>();
        results = Collections.synchronizedSet(new HashSet<>());
        productGraph = new ProductGraph<>(capacity, query);
        this.size = size;
    }

    public Set<ResultPair<Integer>> getResults() {
        return  results;
    }

    public long getResultCount() {
        return resultCounter.getCount();
    }

    public void addMetricRegistry(MetricRegistry metricRegistry) {
        // register all the matrics
        this.metricRegistry = metricRegistry;

        // a counter that keeps track of total result count
        this.resultCounter = metricRegistry.counter("result-counter");

        // histogram that keeps track of processing append only  tuples in teh stream if there is a corresponding edge in the product graph
        this.processedHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("processed-histogram", this.processedHistogram);

        // histogram that keeps track of processing of explicit negative tuples
        this.explicitDeletionHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("explicit-deletion-histogram", this.explicitDeletionHistogram);

        // histogram responsible of tracking how many trees are affected by each input stream edge
        this.containingTreeHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("containing-tree-counter", this.containingTreeHistogram);

        // measures the time spent on processing each edge from the input stream
        this.fullTimer = new Timer(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("full-timer", this.fullTimer);

        // histogram responsible to measure time spent in Window Expiry procedure at every slide interval
        this.windowManagementHistogram = new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));
        metricRegistry.register("window-histogram", windowManagementHistogram);

        // histogram responsible of keeping track number of edges in each side of a window
        edgeCountHistogram = metricRegistry.histogram("edgecount-histogram");

        this.productGraph.addMetricRegistry(metricRegistry);
    }

    public abstract Collection<AbstractTreeNode>[] processEdge(InputTuple<Integer, Integer, L> inputTuple, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWVs_latest);
    public abstract Collection<AbstractTreeNode>[] processNodes(AbstractTreeNode[] nodes, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWVs_latest);
    public abstract Collection<AbstractTreeNode>[] getNodes(HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> new_CWVs);
    public abstract void expiry(long minTimestamp);
    public abstract void shutDown();

    /**
     * Create a windowed RPQ engine ready to execute queries based on given parameters
     * @param query Automata representation of the standing RPQ
     * @param capacity Number of spanning trees and index size
     * @param windowSize Window size in terms of milliseconds
     * @param slideSize Slide size in terms of milliseconds
     * @param numOfThreads Total number of threads for ExpansionExecutor Pool
     * @param semantics arbitrary or simple
     * @param <L> Type of tuple labels and automata transitions
     * @return
     */
}
