package cn.fudan.cs.drpq.stree.engine;

import cn.fudan.cs.drpq.stree.data.AbstractSpanningTree;
import cn.fudan.cs.drpq.stree.data.AbstractTreeNode;
import cn.fudan.cs.drpq.stree.data.ProductGraph;
import cn.fudan.cs.drpq.stree.data.ResultPair;
import cn.fudan.cs.drpq.stree.query.Automata;
import cn.fudan.cs.drpq.stree.util.Constants;
import cn.fudan.cs.drpq.stree.util.Hasher;

import java.util.*;
import java.util.concurrent.Callable;

public abstract class AbstractTreeExpansionJob<L, T extends AbstractSpanningTree<Integer, T, N>, N extends AbstractTreeNode<Integer, T, N>>
        implements Callable<Collection<AbstractTreeNode>[]> {

    protected ProductGraph<Integer,L> productGraph;
    protected Automata<L> automata;
    protected T[] spanningTree;
    protected N[] parentNode;
    protected int[] targetVertex;
    protected int[] targetState;
    protected long[] edgeTimestamp;
    protected long[] labeledTimestamp;
//    protected HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWVs;
    protected int currentSize;
    protected int resultCount;
    protected long windowSize;
    protected T tree;
    protected Set<ResultPair<Integer>> results;
    protected int size;
    protected HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWV_table;

    protected AbstractTreeExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results, long windowsize, T tree) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.results = results;
        this.windowSize = windowsize;
        this.tree = tree;
    }

    protected AbstractTreeExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results, long windowsize) {
        this.productGraph = productGraph;
        this.automata = automata;
        this.targetVertex = new int[Constants.EXPECTED_BATCH_SIZE];
        this.targetState = new int[Constants.EXPECTED_BATCH_SIZE];
        this.edgeTimestamp = new long[Constants.EXPECTED_BATCH_SIZE];
        this.labeledTimestamp = new long[Constants.EXPECTED_BATCH_SIZE];
        this.results = results;
        this.currentSize = 0;
        this.resultCount = 0;
        this.windowSize = windowsize;
    }

    /**
     * Populates the job array
     * @param spanningTree
     * @param parentNode
     * @param targetVertex
     * @param targetState
     * @param edgeTimestamp
     * @return false whenever job array is full and cannot be further populated
     */
    public  boolean addJob(T spanningTree, N parentNode, int targetVertex, int targetState, long edgeTimestamp, long labeledTimestamp) throws IllegalStateException {
        if(this.currentSize >= Constants.EXPECTED_BATCH_SIZE) {
            throw new IllegalStateException("Job capacity exceed limit " + currentSize);
        }
        this.labeledTimestamp[currentSize] = labeledTimestamp;
        this.spanningTree[currentSize] = spanningTree;
        this.parentNode[currentSize] = parentNode;
        this.targetVertex[currentSize] = targetVertex;
        this.targetState[currentSize] = targetState;
        this.edgeTimestamp[currentSize] = edgeTimestamp;
        this.currentSize++;

        if(currentSize == Constants.EXPECTED_BATCH_SIZE - 1) {
            return false;
        }

        return true;
    }

    public boolean addCWVJob(T spanningTree){
        if(this.currentSize >= Constants.EXPECTED_BATCH_SIZE) {
            throw new IllegalStateException("Job capacity exceed limit " + currentSize);
        }
        this.spanningTree[currentSize] = spanningTree;
        this.currentSize++;

        if(currentSize == Constants.EXPECTED_BATCH_SIZE - 1) {
            return false;
        }

        return true;
    }

    /**
     * Determines whether the current batch is full
     * @return
     */
    public boolean isFull() {
        return currentSize == Constants.EXPECTED_BATCH_SIZE - 1;
    }

    /**
     * Determines whether the current batch is empty
     * @return
     */
    public boolean isEmpty() {
        return currentSize == 0;
    }

//    /**
//     * Process a single transition (edge) of the product graph over the spanning tree
//     * @param tree SpanningTree where the transition will be processed over
//     * @param parentNode SpanningTree node as the source of the transition
//     * @param childVertex
//     * @param childState
//     * @param edgeTimestamp
//     */
//    public abstract Collection<AbstractTreeNode> processTransition(T tree, N parentNode, int childVertex, int childState, long edgeTimestamp, long labeledTimestamp);
//    public abstract Collection<AbstractTreeNode> processCWVNode();
//    /**
//     * Explicit deletion processing.
//     * Mark all the nodes in the subtree of the deleted edge with an expired timestamp;
//     * call window expiry procedure
//     * @param tree
//     * @param parentNode
//     * @param childVertex
//     * @param childState
//     * @param timestamp
//     */
//    public abstract void markExpired(T tree, N parentNode, int childVertex, int childState, long timestamp);

    @Override
    public abstract Collection<AbstractTreeNode>[] call() throws Exception;
}
