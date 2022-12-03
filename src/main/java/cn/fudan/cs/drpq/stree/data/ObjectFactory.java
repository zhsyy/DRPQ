package cn.fudan.cs.drpq.stree.data;

import cn.fudan.cs.drpq.stree.engine.AbstractTreeExpansionJob;
import cn.fudan.cs.drpq.stree.query.Automata;
import cn.fudan.cs.drpq.stree.util.Hasher;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

public interface  ObjectFactory<V, T extends AbstractSpanningTree<V, T, N>, N extends AbstractTreeNode<V, T, N>> {

    N createTreeNode(T tree, V vertex, int state);

    T createSpanningTree(Delta<V, T, N> delta, V vertex, int state, V root, LinkedList<Long> upper_bound_timestamp, LinkedList<Long> lower_bound_timestamp);

    <L> AbstractTreeExpansionJob createExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results,
                                                    long windowsize, int size, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWV_table);

    <L> AbstractTreeExpansionJob createCWVExpansionJob(ProductGraph<Integer,L> productGraph, Automata<L> automata, Set<ResultPair<Integer>> results,
                                                       long windowsize, T tree, int size, HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWV_table);
}
