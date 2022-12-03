package cn.fudan.cs.drpq.stree.data.arbitrary;

import cn.fudan.cs.drpq.stree.data.Delta;
import cn.fudan.cs.drpq.stree.data.ObjectFactory;
import cn.fudan.cs.drpq.stree.data.ProductGraph;
import cn.fudan.cs.drpq.stree.data.ResultPair;
import cn.fudan.cs.drpq.stree.engine.AbstractTreeExpansionJob;
import cn.fudan.cs.drpq.stree.engine.CWVNodesExpansionJob;
import cn.fudan.cs.drpq.stree.engine.TreeNodeRAPQTreeExpansionJob;
import cn.fudan.cs.drpq.stree.query.Automata;
import cn.fudan.cs.drpq.stree.util.Hasher;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public class ObjectFactoryArbitrary<V> implements ObjectFactory<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> {
    @Override
    public TreeNodeRAPQ<V> createTreeNode(SpanningTreeRAPQ<V> tree, V vertex, int state) {
        return new TreeNodeRAPQ<V>(vertex, state, new LinkedList<>(), new LinkedList<>());
    }

    @Override
    public SpanningTreeRAPQ<V> createSpanningTree(Delta<V, SpanningTreeRAPQ<V>, TreeNodeRAPQ<V>> delta, V vertex, int state, V root,
                                                  LinkedList<Long> lower_bound_timestamp, LinkedList<Long> upper_bound_timestamp) {
        return new SpanningTreeRAPQ<V>(delta, vertex, state, root, lower_bound_timestamp, upper_bound_timestamp);
    }

    @Override
    public <L> AbstractTreeExpansionJob createExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results,
                                                           long windowsize, int size, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWV_table) {
        return new TreeNodeRAPQTreeExpansionJob<>(productGraph, automata, results, windowsize, size, CWV_table);
    }

    @Override
    public <L> AbstractTreeExpansionJob createCWVExpansionJob(ProductGraph<Integer, L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results,
                                                           long windowsize, SpanningTreeRAPQ<V> tree, int size, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWV_table) {
        return new CWVNodesExpansionJob(productGraph, automata, results, windowsize, tree, size, CWV_table);
    }
}
