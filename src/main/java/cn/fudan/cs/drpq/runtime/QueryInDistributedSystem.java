package cn.fudan.cs.drpq.runtime;

import cn.fudan.cs.drpq.input.InputTuple;
import cn.fudan.cs.drpq.input.SimpleTextStreamWithExplicitDeletions;
import cn.fudan.cs.drpq.input.TextFileStream;
import cn.fudan.cs.drpq.stree.data.AbstractTreeNode;
import cn.fudan.cs.drpq.stree.data.ManualQueryAutomata;
import cn.fudan.cs.drpq.stree.data.arbitrary.SpanningTreeRAPQ;
import cn.fudan.cs.drpq.stree.data.arbitrary.TreeNodeRAPQ;
import cn.fudan.cs.drpq.stree.engine.RPQEngine;
import cn.fudan.cs.drpq.stree.engine.WindowedRPQ;
import cn.fudan.cs.drpq.stree.util.Constants;
import cn.fudan.cs.drpq.stree.util.Hasher;
import cn.fudan.cs.drpq.stree.util.Semantics;
import com.google.common.collect.HashMultimap;
import mpi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class QueryInDistributedSystem {
    static HashMap<Integer, int[]> partial_degree = new HashMap<>();

    static long p;
    static long windowSize;
    static boolean ifExpire;
    static int throughput;
    static int batch_size;
    static int expire_per_batch;
    static int query_index;

    static String filename = "Yago3.txt";
    static double real_throughput = 0;
    static boolean ifCountLETime = false;
    static boolean ifWorkerExpire = false;
    static long LE_time = Long.MAX_VALUE;
    static final Logger logger = LoggerFactory.getLogger(QueryInDistributedSystem.class);
    static int total_send_count = 0;
    static int local_send_count = 0;

    static int Second = 0;
    static InputTuple current_processing_tuple;
    static AbstractTreeNode[] current_processing_node;
    static String metric = "";

    public static void main(String[] args) throws InterruptedException {
        String appArgs[] = MPI.Init(args);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        Group group = MPI.COMM_WORLD.Group().Excl(new int[]{0});
        Intracomm workers_Intracomm = MPI.COMM_WORLD.Create(group);
        Random random_partition = new Random(1);

        int K_hop = 1;
        int waiting_time_expire = 10;
        int waiting_time = 100;
        int partition = 0;
        boolean test = false;

        if (args.length > 0) {
            windowSize = Long.parseLong(appArgs[0]);
            throughput = Integer.parseInt(appArgs[1]);
            batch_size = Integer.parseInt(appArgs[2]);
            query_index = Integer.parseInt(appArgs[3]);
            partition = Integer.parseInt(appArgs[4]);

            ifExpire = Integer.parseInt(appArgs[5]) == 1;
            expire_per_batch = Integer.parseInt(appArgs[6]);
            if (ifExpire) {
                waiting_time_expire = Integer.parseInt(appArgs[7]);
            } else
                waiting_time = Integer.parseInt(appArgs[7]);
            K_hop = Integer.parseInt(appArgs[8]);
            p = Long.parseLong(appArgs[9]);
            filename = appArgs[11];
        }

        /* construct queries */
        ManualQueryAutomata<String> query;
        switch (query_index){
            case 0:
                if (Integer.parseInt(appArgs[10]) == 0) {
                    query = new ManualQueryAutomata<String>(6);
                    query.addFinalState(5);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "a", 2);
                    query.addTransition(2, "a", 3);
                    query.addTransition(3, "a", 4);
                    query.addTransition(4, "a", 5);
                } else {
                    query = new ManualQueryAutomata<String>(2);
                    query.addFinalState(1);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "a", 1);
                }
                break;
            case 1:// (a b)*
                if (Integer.parseInt(appArgs[10]) == 0) {
                    query = new ManualQueryAutomata<String>(6);
                    query.addFinalState(5);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "b", 2);
                    query.addTransition(2, "a", 3);
                    query.addTransition(3, "b", 4);
                    query.addTransition(4, "a", 5);
                } else {
                    query = new ManualQueryAutomata<String>(3);
                    query.addFinalState(2);
                    query.addTransition(0, "c", 1);
                    query.addTransition(1, "a", 2);
                    query.addTransition(2, "c", 1);
                }
                break;
            case 2:// a b*
                if (Integer.parseInt(appArgs[10]) == 0) {
                    query = new ManualQueryAutomata<String>(6);
                    query.addFinalState(5);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "b", 2);
                    query.addTransition(2, "b", 3);
                    query.addTransition(3, "b", 4);
                    query.addTransition(4, "b", 5);
                } else {
                    query = new ManualQueryAutomata<String>(3);
                    query.addFinalState(2);
                    query.addTransition(0, "d", 1);
                    query.addTransition(1, "a", 2);
                    query.addTransition(2, "a", 2);
                }
                break;
            case 3:// a b* c
                if (Integer.parseInt(appArgs[10]) == 0) {
                    query = new ManualQueryAutomata<String>(6);
                    query.addFinalState(5);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "b", 2);
                    query.addTransition(2, "b", 3);
                    query.addTransition(3, "b", 4);
                    query.addTransition(4, "c", 5);
                } else {
                    query = new ManualQueryAutomata<String>(4);
                    query.addFinalState(3);
                    query.addTransition(0, "d", 1);
                    query.addTransition(1, "a", 2);
                    query.addTransition(2, "a", 2);
                    query.addTransition(2, "87", 3);
                }
                break;
            case 4:// a b c*
                if (Integer.parseInt(appArgs[10]) == 0) {
                    query = new ManualQueryAutomata<String>(4);
                    query.addFinalState(2);
                    query.addFinalState(3);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "b", 2);
                    query.addTransition(2, "c", 3);
                    query.addTransition(3, "c", 3);
                } else {
                    query = new ManualQueryAutomata<String>(4);
                    query.addFinalState(3);
                    query.addTransition(0, "d", 1);
                    query.addTransition(1, "a", 2);
                    query.addTransition(2, "87", 3);
                    query.addTransition(3, "87", 3);
                }
                break;
            case 5:// a b* c*
                if (Integer.parseInt(appArgs[10]) == 0) {
                    query = new ManualQueryAutomata<String>(10);
                    query.addFinalState(5);
                    query.addFinalState(9);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "b", 2);
                    query.addTransition(2, "b", 3);
                    query.addTransition(3, "b", 4);
                    query.addTransition(4, "b", 5);
                    query.addTransition(1, "c", 6);
                    query.addTransition(6, "c", 7);
                    query.addTransition(7, "c", 8);
                    query.addTransition(8, "c", 9);
                    query.addTransition(2, "c", 7);
                    query.addTransition(3, "c", 8);
                    query.addTransition(4, "c", 9);
                } else {
                    query = new ManualQueryAutomata<String>(4);
                    query.addFinalState(2);
                    query.addFinalState(3);
                    query.addTransition(0, "d", 1);
                    query.addTransition(1, "a", 2);
                    query.addTransition(2, "a", 2);
                    query.addTransition(1, "87", 3);
                    query.addTransition(2, "87", 3);
                    query.addTransition(3, "87", 3);
                }
                break;
            case 6:// (a+ b+ c+ d+) e*
                if (Integer.parseInt(appArgs[10]) == 0) {
                    query = new ManualQueryAutomata<String>(9);
                    query.addFinalState(5);
                    query.addTransition(0, "a", 1);
                    query.addTransition(1, "a", 2);
                    query.addTransition(2, "a", 3);
                    query.addTransition(3, "b", 4);
                    query.addTransition(4, "c", 5);
                    query.addTransition(1, "b", 7);
                    query.addTransition(7, "b", 3);
                    query.addTransition(2, "b", 6);
                    query.addTransition(6, "b", 4);
                    query.addTransition(7, "c", 8);
                    query.addTransition(8, "c", 4);
                    query.addTransition(3, "c", 4);
                    query.addTransition(6, "c", 4);
                }
                else {
                    query = new ManualQueryAutomata<String>(6);
                    query.addFinalState(5);
                    query.addTransition(0, "3", 1);
                    query.addTransition(0, "3", 2);
                    query.addTransition(1, "3", 2);
                    query.addTransition(0, "d", 3);
                    query.addTransition(1, "d", 3);
                    query.addTransition(2, "d", 3);
                    query.addTransition(0, "80", 4);
                    query.addTransition(1, "80", 4);
                    query.addTransition(2, "80", 4);
                    query.addTransition(3, "80", 4);
                    query.addTransition(1, "a", 5);
                    query.addTransition(2, "a", 5);
                    query.addTransition(3, "a", 5);
                    query.addTransition(4, "a", 5);
                    query.addTransition(5, "a", 5);
                }
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + query_index);
        }

        if (rank == 0) {
            System.out.println("Rank Size: <"+size+"> windowsize: " + windowSize + " batch size: "
                    + batch_size + " query index: " + query_index + " input file: "+ filename);

            /* From-vertex -> (worker : latest ts)*/
            HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> from_vertex2workers_and_times = new HashMap<>();
            /* To-vertex -> (worker : latest ts)*/
            HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> to_vertex2workers_and_times = new HashMap<>();
            /* Label -> tuples */
            HashMultimap<String, InputTuple<Integer, Integer, String>>[] label2tuples = new HashMultimap[size];
            /* vertex -> label, hop to node count*/
            HashMap<Hasher.MapKey<Integer>, HashMap<Hasher.MapKey<Integer>, Long>>[] node_PLS_table = new HashMap[size];

            HashMap<Hasher.MapKey<Integer>, int[]> from_vertex2worker_count = new HashMap<>();
            HashMap<Hasher.MapKey<Integer>, int[]> to_vertex2worker_count = new HashMap<>();

            /* 存因为存在cwv需要发送给worker的信息 CWV -> (worker :　latest ts) */
            HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>>[] Send_vertex2workers = new HashMap[size];
            /* 存需要发送给worker的tuples */
            ArrayList<InputTuple<Integer, Integer, String>>[] Send_tuples = new ArrayList[size];
            /* load count */
            int[] load_count = new int[size];
            /* worker -> nodes */
            LinkedList<Hasher.MapKey<Integer>>[] worker2nodes = new LinkedList[size];
            /* worker : from-vertex -> count */
            HashMap<Integer, Integer>[] from_vertex_to_count = new HashMap[size];
            /* worker : to-vertex -> count */
            HashMap<Integer, Integer>[] to_vertex_to_count = new HashMap[size];

            ConcurrentLinkedQueue<InputTuple<Integer, Integer, String>> throughput_tuples = new ConcurrentLinkedQueue<>();

            long GE_time;
            long process_begin;
            AtomicLong process_end = new AtomicLong();
            long last_expiration_time = 0;

            HashMap<Long, Long> label_ts_to_clock_ts = new HashMap<>();
            HashMap<Long, LinkedList<Long>> label_ts_to_clock_ts_list = new HashMap<>();
            long current_label_ts = Long.MIN_VALUE;
            long current_label_ts_total_send_clock_ts = 0;
            int current_label_ts_count = 0;
            LinkedList<Long> current_sending_label_ts = new LinkedList<>();

            /*initialize*/
            for (int i = 1; i < size; i++) {
                label2tuples[i] = HashMultimap.create();
                node_PLS_table[i] = new HashMap<>();
                Send_vertex2workers[i] = new HashMap<>();
                Send_tuples[i] = new ArrayList<>();
                worker2nodes[i] = new LinkedList<>();
                from_vertex_to_count[i] = new HashMap<>();
                to_vertex_to_count[i] = new HashMap<>();
            }

            int send_count = 0;
            TextFileStream<Integer, Integer, String> stream = new SimpleTextStreamWithExplicitDeletions();
            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            stream.open(filename);
//            InputTuple<Integer, Integer, String> pre_read_input = stream.next();
//            LinkedList<InputTuple<Integer, Integer, String>> pre_read_list = new LinkedList<>();
//            while (pre_read_input != null) {
//                pre_read_list.add(pre_read_input);
//                pre_read_input = stream.next();
//            }
//            int send_size = pre_read_list.size();

            process_begin = System.nanoTime();
            Runnable system_throughput = () -> {
//                int count = 0;
//                while (!pre_read_list.isEmpty()) {
//                    throughput_tuples.add(pre_read_list.removeFirst());
//                    count++;
//                    if (count > throughput / 5) {
//                        break;
//                    }
//                }
//                if (count == 0){
//                    process_end.set(System.nanoTime());
//                    real_throughput = send_size / ((process_end.get() - process_begin) * 1.0 / 1000000000);
//                    System.out.println("Send finish from input, avg rate = " + real_throughput);
//                    executor.shutdown();
//                }

                InputTuple<Integer, Integer, String> input = stream.next();
                if (input != null) {
                    throughput_tuples.add(input);
                } else {
                    System.out.println("Send finish, avg rate = " + 5628166 / ((System.nanoTime() - process_begin) * 1.0 / 1000000000));
                    process_end.set(System.nanoTime());
                    real_throughput = 5628166 / ((process_end.get() - process_begin) * 1.0 / 1000000000);
                    executor.shutdown();
                }
            };
//            executor.scheduleAtFixedRate(system_throughput, 0, 200, TimeUnit.MILLISECONDS);
            executor.scheduleAtFixedRate(system_throughput, 0, 1000000/throughput, TimeUnit.MICROSECONDS);

            while (!throughput_tuples.isEmpty() || !executor.isShutdown()) {
                InputTuple<Integer, Integer, String> input = throughput_tuples.poll();
                if (input != null) {
                    long labelTs = input.getTimestamp();
                    String label = input.getLabel();
                    int source = input.getSource();
                    int target = input.getTarget();

                    GE_time = labelTs;

                    /* QGPartition */
                    Map<Integer, Integer> transitions = query.getTransition(label);
                    if (transitions.isEmpty())
                        continue;
                    List<Map.Entry<Integer, Integer>> transitionList = new ArrayList<>(transitions.entrySet());

                    int target_worker = -1;


                    // Todo our approach

                    switch (partition){
                        case 1:
                            /* random */
                            target_worker = random_partition.nextInt(size - 1) + 1;
                            break;
                        case 2:
                            /* Greedy */
                            target_worker = Greedy(source, target, size, from_vertex2workers_and_times, to_vertex2workers_and_times, load_count, transitionList);
                            break;
                        case 3:
                            /*  HDRF */
                            target_worker = hdrf(source, target, size, load_count);
                            break;
                        case 4:
                            /* DBH */
                            target_worker = DBH(source, target, size);
                            break;
                        case 5:
                            /* DBH */
                            target_worker = Grid(source, target, size, load_count);
                            break;
                        case 6:
                            /* QGPartition */
                            for (int i = 1; i < size; i++) {
                                if (label2tuples[i].get(label).contains(input)) {
                                    target_worker = i;
                                    break;
                                }
                            }
                            if (target_worker == -1) {

                                int[] one_hop_count = new int[size];
                                int[] two_hop_count = new int[size];
                                for (int i = 1; i < size; i++) {
                                    for (Map.Entry<Integer, Integer> e : transitionList) {
                                        if (to_vertex2worker_count.containsKey(Hasher.getThreadLocalTreeNodePairKey(source, e.getKey())))
                                            one_hop_count[i] += to_vertex2worker_count.get(Hasher.getThreadLocalTreeNodePairKey(source, e.getKey()))[i];
                                        if (K_hop == 2){
                                            HashMap<Hasher.MapKey<Integer>, Long> mapKeyCollection = node_PLS_table[i].get(Hasher.getThreadLocalTreeNodePairKey(source, e.getKey()));
                                            if (mapKeyCollection != null) {
                                                for (Hasher.MapKey<Integer> mapKey : mapKeyCollection.keySet()) {
                                                    HashMap<Hasher.MapKey<Integer>, Long> mapKeys = node_PLS_table[i].get(Hasher.getThreadLocalTreeNodePairKey(mapKey.X, mapKey.Y));
                                                    if (mapKeys != null)
                                                        two_hop_count[i] += mapKeys.size();
                                                }
                                            }
                                        }
                                    }
                                }

                                long max_one_hop = 0;
                                int min_one_hop = Integer.MAX_VALUE;
                                int max_two_hop = 0;
                                int min_two_hop = Integer.MAX_VALUE;
                                int max_load = 0;
                                int min_load = Integer.MAX_VALUE;


                                for (int i = 1; i < size; i++) {
                                    long hop = one_hop_count[i] + (100 / p) * two_hop_count[i];
                                    if (hop > max_one_hop) {
                                        max_one_hop = hop;
                                        target_worker = i;
                                    }
                                }

                                if (target_worker == -1) {
                                    target_worker = Greedy(source, target, size, from_vertex2workers_and_times, to_vertex2workers_and_times, load_count, transitionList);
                                }

                                /* Sample*/
                                if (random_partition.nextInt(100) < p) {
                                    for (Map.Entry<Integer, Integer> e : transitionList) {
                                        if (!node_PLS_table[target_worker].containsKey(Hasher.getThreadLocalTreeNodePairKey(target, e.getValue()))){
                                            node_PLS_table[target_worker].put(Hasher.createTreeNodePairKey(target, e.getValue()), new HashMap<>());
                                        }
//                                        else {
//                                            node_PLS_table[target_worker].put(Hasher.createTreeNodePairKey(target, e.getValue()), Hasher.createTreeNodePairKey(source, e.getKey()));
//                                        }
                                        node_PLS_table[target_worker].get(Hasher.createTreeNodePairKey(target, e.getValue())).put(Hasher.createTreeNodePairKey(source, e.getKey()), labelTs);
                                    }
                                    label2tuples[target_worker].put(label, input);
                                }
                            }
                            break;

                    }


                    /*加入到待发送的队列中*/
                    Send_tuples[target_worker].add(input);

                    /* maintain degree*/
                    partial_degree.computeIfAbsent(source, key -> new int[size]);
                    partial_degree.get(source)[target_worker]++;
                    partial_degree.computeIfAbsent(target, key -> new int[size]);
                    partial_degree.get(target)[target_worker]++;

                    /*Tuple Forward方法*/
                    for (Map.Entry<Integer, Integer> e : transitionList) {
                        Hasher.MapKey<Integer> mapKey_1 = Hasher.createTreeNodePairKey(source, e.getKey());
                        /*1. from vertex is CWV*/
                        LinkedList<Long>[] workers2Ts = to_vertex2workers_and_times.get(mapKey_1);
                        if (workers2Ts != null) {
                            for (int worker = 1; worker < workers2Ts.length; worker++) {
                                if (worker != target_worker && workers2Ts[worker] != null && !workers2Ts[worker].isEmpty()
                                        && workers2Ts[worker].getLast() > labelTs - windowSize) {
                                    /* 确定是需要插入的 */
                                    LinkedList<Long> linkedList = Send_vertex2workers[worker]
                                            .computeIfAbsent(mapKey_1, key -> new HashMap<>())
                                            .computeIfAbsent(target_worker, key -> new LinkedList<>());
                                    if (linkedList.isEmpty() || labelTs > linkedList.getLast()){
                                        linkedList.add(labelTs);
                                    }
                                }
                            }
                        }

                        Hasher.MapKey<Integer> mapKey_2 = Hasher.createTreeNodePairKey(target, e.getValue());
                        /*2. to vertex is CWV*/
                        workers2Ts = from_vertex2workers_and_times.get(mapKey_2);
                        if (workers2Ts != null) {
                            for (int worker = 1; worker < workers2Ts.length; worker++) {
                                if (worker != target_worker && workers2Ts[worker] != null && !workers2Ts[worker].isEmpty()) {
                                    for (Long ts : workers2Ts[worker]) {
                                        if (ts > labelTs - windowSize) {
                                            LinkedList<Long> linkedList = Send_vertex2workers[target_worker]
                                                    .computeIfAbsent(mapKey_2, key -> new HashMap<>())
                                                    .computeIfAbsent(worker, key -> new LinkedList<>());
                                            if (linkedList.isEmpty() || ts > linkedList.getLast()) {
                                                linkedList.add(ts);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    /* 维护vertex和它对应的worker和label ts */
                    for (Map.Entry<Integer, Integer> e : transitionList) {
                        int source_state = e.getKey();
                        int target_state = e.getValue();
                        Hasher.MapKey<Integer> mapKey_1 = Hasher.createTreeNodePairKey(source, source_state);
                        LinkedList<Long>[] linkedLists_from = from_vertex2workers_and_times.computeIfAbsent(mapKey_1, key -> new LinkedList[size]);
                        if (linkedLists_from[target_worker] == null || linkedLists_from[target_worker].isEmpty()) {
                            linkedLists_from[target_worker] = new LinkedList<>();
                            linkedLists_from[target_worker].add(labelTs);
                        }
                        else if (linkedLists_from[target_worker].getLast() < labelTs)
                            linkedLists_from[target_worker].add(labelTs);

                        /* maintain count */
                        if (!from_vertex2worker_count.containsKey(mapKey_1))
                            from_vertex2worker_count.put(mapKey_1, new int[size]);
                        from_vertex2worker_count.get(mapKey_1)[target_worker]++;

                        Hasher.MapKey<Integer> mapKey_2 = Hasher.createTreeNodePairKey(target, target_state);
                        LinkedList<Long>[] linkedLists_to = to_vertex2workers_and_times.computeIfAbsent(mapKey_2, key -> new LinkedList[size]);
                        if (linkedLists_to[target_worker] == null || linkedLists_to[target_worker].isEmpty()) {
                            linkedLists_to[target_worker] = new LinkedList<>();
                            linkedLists_to[target_worker].add(labelTs);
                        }
                        else if (linkedLists_to[target_worker].getLast() < labelTs)
                            linkedLists_to[target_worker].add(labelTs);

                        /* maintain count */
                        if (!to_vertex2worker_count.containsKey(mapKey_2))
                            to_vertex2worker_count.put(mapKey_2, new int[size]);
                        to_vertex2worker_count.get(mapKey_2)[target_worker]++;
                    }

                    /* maintain load count */
                    load_count[target_worker]++;

                    send_count++;
                    current_sending_label_ts.add(labelTs);

                    /*communication*/
                    if (send_count >= batch_size) {
                        local_send_count += send_count;
                        total_send_count += send_count;
                        send_count = 0;
                        /*send msg to worker i*/
                        for (int i = 1; i < size; i++) {
                            if (Send_tuples[i].size() > 0) {
                                InputTuple[] inputTuples = Send_tuples[i].toArray(new InputTuple[0]);
                                MPI.COMM_WORLD.Send(inputTuples, 0, inputTuples.length, MPI.OBJECT, i, Constants.TUPLES);
                                Send_tuples[i].clear();
                            }
                        }
                        for (int i = 1; i < size; i++) {
                            if (Send_vertex2workers[i].size() > 0) {
                                // vertex, state, worker ts....
                                MPI.COMM_WORLD.Send(new HashMap[]{Send_vertex2workers[i]}, 0, 1, MPI.OBJECT, i, Constants.CWV2WORKERS);
                                Send_vertex2workers[i].clear();
                            }
                        }

                        /* count sending clock time (avg for each labeled ts)*/
                        long send_ts = System.currentTimeMillis();
                        for (long ts : current_sending_label_ts) {
                            if (!label_ts_to_clock_ts_list.containsKey(ts))
                                label_ts_to_clock_ts_list.put(ts, new LinkedList());
                            label_ts_to_clock_ts_list.get(ts).add(send_ts);
                        }
                        current_sending_label_ts.clear();

                        /* Expiration */
                        if (ifExpire && total_send_count % (batch_size * expire_per_batch) == 0) {
//                            System.out.println("Begin Expiration");
                            for (int i = 1; i < size; i++) {
                                MPI.COMM_WORLD.Isend(new int[0], 0, 0, MPI.INT, i, Constants.EXPIRATION_REQUEST);
                            }

                            for (int i = 1; i < size; i++) {
                                long[] LE_time = new long[1];
                                MPI.COMM_WORLD.Recv(LE_time, 0, 1, MPI.LONG, i, Constants.EXPIRATION_TIME);
//                                System.out.println("Get LE_time From rank " + i);
                                GE_time = Long.min(LE_time[0], GE_time);
                            }
                            for (int i = 1; i < size; i++) {
                                MPI.COMM_WORLD.Send(new long[]{GE_time - windowSize}, 0, 1, MPI.LONG, i, Constants.EXPIRATION_TIME);
//                                System.out.println("Send Expire Time " + (GE_time - windowSize) + " to " + i);
                            }


                            if (last_expiration_time < GE_time) {
                                expireForMaster(GE_time - windowSize, label2tuples, from_vertex2workers_and_times, to_vertex2workers_and_times, node_PLS_table);
                                last_expiration_time = GE_time;
                            }
                        }
                    }
                }
            }

            /*send the rest tuples to worker i*/
            if (send_count != 0){
                for (int i = 1; i < size; i++) {
                    if (Send_tuples[i].size() > 0) {
                        InputTuple[] inputTuples = Send_tuples[i].toArray(new InputTuple[0]);
                        MPI.COMM_WORLD.Send(inputTuples, 0, inputTuples.length, MPI.OBJECT, i, Constants.TUPLES);
                        Send_tuples[i].clear();
                    }
                }
                for (int i = 1; i < size; i++) {
                    if (Send_vertex2workers[i].size() > 0) {
                        MPI.COMM_WORLD.Send(new HashMap[]{Send_vertex2workers[i]}, 0, 1, MPI.OBJECT, i, Constants.CWV2WORKERS);
                        Send_vertex2workers[i].clear();
                    }
                }
                /* count sending clock time (avg for each labeled ts)*/
                long send_ts = System.currentTimeMillis();
                for (long ts : current_sending_label_ts) {
                    if (!label_ts_to_clock_ts_list.containsKey(ts))
                        label_ts_to_clock_ts_list.put(ts, new LinkedList());
                    label_ts_to_clock_ts_list.get(ts).add(send_ts);
                }
            }

            System.out.println("Send finish for last send, time = " +  (System.nanoTime() - process_end.get()) * 1.0 / 1000000000);

            if (current_label_ts_count != 0)
                label_ts_to_clock_ts.put(current_label_ts, current_label_ts_total_send_clock_ts / current_label_ts_count);
//            stream.close();
            /*send the finish tag to all workers */
            if (ifExpire)
                TimeUnit.SECONDS.sleep(waiting_time_expire);
            else
                TimeUnit.SECONDS.sleep(waiting_time);
            for (int i = 1; i < size; i++) {
                MPI.COMM_WORLD.Isend(new int[0], 0, 0, MPI.INT, i, Constants.FINISH);
//                System.out.println("Coordinator sends Finish-Tag to worker" + i);
            }
            MPI.COMM_WORLD.Barrier();

            int[] receive_nodes_count = new int[2];
            for (int i = 1; i < size; i++) {
                Status status = MPI.COMM_WORLD.Probe(MPI.ANY_SOURCE, Constants.FINISH);
                int[] receive_msg = new int[status.count];
                MPI.COMM_WORLD.Recv(receive_msg, 0, status.count, MPI.INT, status.source, Constants.FINISH);
                for (int j = 0; j < receive_msg.length; j++) {
                    receive_nodes_count[j] += receive_msg[j];
                }
            }

            HashMap<String, Object> latency_metric = new HashMap<>();
//            latency_metric.put("mean", latency_histogram.getSnapshot().getMean());
//            latency_metric.put("75th", latency_histogram.getSnapshot().get75thPercentile());
//            latency_metric.put("98th", latency_histogram.getSnapshot().get98thPercentile());
//            latency_metric.put("99th", latency_histogram.getSnapshot().get99thPercentile());

            HashMap<String, Object> node_send_count = new HashMap<>();
            node_send_count.put("CWV node (sum)", receive_nodes_count[0]);
            node_send_count.put("EDGE node (sum)", receive_nodes_count[1]);
            node_send_count.put("all node (sum)", receive_nodes_count[0] + receive_nodes_count[1]);
            ArrayList<HashMap> hashMaps = new ArrayList<>();

            for (int i = 1; i < size; i++) {
                Status status = MPI.COMM_WORLD.Probe(MPI.ANY_SOURCE, Constants.METRIC);
                HashMap[] receive_msg = new HashMap[status.count];
                MPI.COMM_WORLD.Recv(receive_msg, 0, status.count, MPI.OBJECT, status.source, Constants.METRIC);
                hashMaps.add(receive_msg[0]);
            }
            if (!test){
                try {
                    BufferedWriter out = new BufferedWriter(new FileWriter("./result.txt", true));
                    out.write("run bash with real-throughput = " + real_throughput + " window_size = " + windowSize + " throughput = " + throughput + " batch_size = " + batch_size + " query_index = " + query_index
                            + " partition = " + partition + " worker count = " + (size - 1)
                            + " hop = " + K_hop + " p = " + p + " expire = " + ifExpire + " expire_per_batch = " + expire_per_batch + "\n");
                    if (!latency_metric.isEmpty())
                        out.write(latency_metric.toString() + " \n ");
                    for (HashMap hashMap : hashMaps) {
                        out.write(hashMap.toString() + " \n");
                    }
                    out.write(node_send_count.toString() + " \n");
                    out.write(" \n");
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            RPQEngine<String> rapqEngine = new WindowedRPQ<String, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>(query, 10000, windowSize, 1,
                    30, Semantics.ARBITRARY, size, rank);

            /* CWV -> (worker : ts), only record to vertex*/
            /* CWVs_latest check if expiration */
            HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> CWVs_latest = new HashMap<>();
            /* last send ts record*/
//            HashMap<Hasher.MapKey<Integer>, long[]> CWVs_latest_send_ts = new HashMap<>();
            /* CWVs_oldest check if need to send */
            ConcurrentLinkedQueue<Status> message_status = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<InputTuple<Integer, Integer, String>[]> tuple_queue = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<AbstractTreeNode[]> node_queue = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<AbstractTreeNode[]> bak_node_queue_for = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>>> CWVs_queue = new ConcurrentLinkedQueue<>();
            long[] Expiration_TS = new long[1];
            long last_expiration_time = 0;
//            AtomicLong LE_time = new AtomicLong(Long.MAX_VALUE);
            /* metric */
            int receive_tuple = 0;
            int receive_node = 0;
//            long maintain_CWV_table_ts = 0;
//            AtomicBoolean flag = new AtomicBoolean(false);

            ScheduledExecutorService executor = null;
            if (test) {
                executor = Executors.newScheduledThreadPool(1);
                Runnable system_throughput = () -> {
                    System.out.println(Second + " -> rank: " + rank + " message_status " + message_status.size() + " tuple_queue " + tuple_queue.size()
                            + " node_queue " + node_queue.size() + " CWVs_queue " + CWVs_queue.size() + " metric " + metric);
                    Second += 2;
                };
                executor.scheduleAtFixedRate(system_throughput, 1, 2, TimeUnit.SECONDS);
            }


            /*开一个线程用于收消息*/
            Thread receive_messages = new Thread(() -> {
                while (true){
                    try{
                        Status status = MPI.COMM_WORLD.Probe(MPI.ANY_SOURCE, MPI.ANY_TAG);
                        switch (status.tag){
                            case Constants.TUPLES:
                                InputTuple<Integer, Integer, String>[] received_msgs = new InputTuple[status.count];
                                MPI.COMM_WORLD.Recv(received_msgs, 0, status.count, MPI.OBJECT, 0, Constants.TUPLES);
                                tuple_queue.offer(received_msgs);
                                break;
                            case Constants.CWV2WORKERS:
                                HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>>[] received_CWVs = new HashMap[status.count];
                                MPI.COMM_WORLD.Recv(received_CWVs, 0, status.count, MPI.OBJECT, MPI.ANY_SOURCE, Constants.CWV2WORKERS);
                                CWVs_queue.offer(received_CWVs[0]);
                                break;
                            case Constants.NODES:
                                TreeNodeRAPQ[] abstractTreeNodes = new TreeNodeRAPQ[status.count];
                                MPI.COMM_WORLD.Recv(abstractTreeNodes, 0, status.count, MPI.OBJECT, MPI.ANY_SOURCE, Constants.NODES);
                                node_queue.offer(abstractTreeNodes);
                                if (ifCountLETime){
                                    for (TreeNodeRAPQ abstractTreeNode : abstractTreeNodes) {
                                        if (abstractTreeNode != null && LE_time > abstractTreeNode.getLabeledTimestamp()) {
                                            LE_time = abstractTreeNode.getLabeledTimestamp();
                                        }
                                    }
                                }
                                break;
                            case Constants.FINISH:
                                MPI.COMM_WORLD.Recv(new int[0], 0, status.count, MPI.INT, 0, Constants.FINISH);
//                                System.out.println("Finish rank " + rank +" node queue " + node_queue.size() + " input queue " + tuple_queue.size());
                                message_status.offer(status);
                                break;
                            case Constants.EXPIRATION_REQUEST:
                                MPI.COMM_WORLD.Recv(new int[0], 0, status.count, MPI.INT, 0, Constants.EXPIRATION_REQUEST);
                                ifCountLETime = true;
//                                need_process_nodes = node_queue.size()
                                node_queue.offer(new AbstractTreeNode[0]);
//                                System.out.println("rank " + rank +" node queue " + node_queue.size() + " input queue " + tuple_queue.size());
                                message_status.offer(status);
                                break;
                            case Constants.EXPIRATION_TIME:
                                MPI.COMM_WORLD.Recv(Expiration_TS, 0, status.count, MPI.LONG, 0, Constants.EXPIRATION_TIME);
                                ifWorkerExpire = true;
//                                message_status.offer(status);
                                break;
                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                        break;
                    }
                }
            });
            receive_messages.start();
            boolean Finish = false;

            // metrics
            int[] received_msg = new int[5];
            int[] node_send_count_msg = new int[2];
            long[] processing_ts = new long[5];
            long begin = 0;
            long run_begin = System.nanoTime();
            /* 发送node的列表 */
            ArrayList<AbstractTreeNode>[] Send_nodes = new ArrayList[size];
            for (int i = 1; i < size; i++) {
                Send_nodes[i] = new ArrayList<>();
            }
            while (!Finish){
                if (ifWorkerExpire) {
                    begin = System.nanoTime();
                    if (Expiration_TS[0] > last_expiration_time) {
//                        System.out.println(rank + " expire as " + Expiration_TS[0]);
                        rapqEngine.expiry(Expiration_TS[0]);
                        Iterator<Hasher.MapKey<Integer>> mapKeyIterator = CWVs_latest.keySet().iterator();
                        while (mapKeyIterator.hasNext()){
                            Hasher.MapKey<Integer> key = mapKeyIterator.next();
                            Map<Integer, LinkedList<Long>> ts = CWVs_latest.get(key);
                            ts.keySet().removeIf(integer -> ts.get(integer).getLast() <= Expiration_TS[0]);
                            if (ts.isEmpty()) {
                                mapKeyIterator.remove();
                            }
                        }
                        last_expiration_time = Expiration_TS[0];
                    }
                    processing_ts[4] += System.nanoTime() - begin;
                    received_msg[4]++;
                    ifWorkerExpire = false;
                }
                if (!node_queue.isEmpty() && node_queue.peek().length != 0){
                    AbstractTreeNode[] received_nodes = node_queue.poll();
                    receive_node += received_nodes.length;
                    begin = System.nanoTime();
                    metric = "Nodes: " + received_nodes.length +" (total: "+ receive_node + " )";
                    Collection<AbstractTreeNode>[] nodes = rapqEngine.processNodes(received_nodes, CWVs_latest);
                    for (int j = 1; j < size; j++) {
                        node_send_count_msg[1] += nodes[j].size();
                        Send_nodes[j].addAll(nodes[j]);
                    }
                    SendNodesAmongWorkers(rank, size, Send_nodes);
                    processing_ts[2] += System.nanoTime() - begin;
                    received_msg[2]+=received_nodes.length;
                }
                else if (!tuple_queue.isEmpty()) {
                    InputTuple<Integer, Integer, String>[] inputTuples = tuple_queue.poll();
                    receive_tuple += inputTuples.length;
                    begin = System.nanoTime();
                    for (int i = 0; i < inputTuples.length; i++) {
                        InputTuple<Integer, Integer, String> inputTuple = inputTuples[i];
                        metric = "Tuple: " + (i+1) +" / "+ inputTuples.length + " (total: " + receive_tuple + ")";
                        Collection<AbstractTreeNode>[] nodes = rapqEngine.processEdge(inputTuple, CWVs_latest);

                        for (int j = 1; j < size; j++) {
                            node_send_count_msg[0] += nodes[j].size();
                            Send_nodes[j].addAll(nodes[j]);
                        }
                    }
                    SendNodesAmongWorkers(rank, size, Send_nodes);
                    processing_ts[0] += System.nanoTime() - begin;
                    received_msg[0]+=inputTuples.length;
                    if (!CWVs_queue.isEmpty()) {
                        HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> new_CWVs = CWVs_queue.poll();
                        begin = System.nanoTime();
                        Collection<AbstractTreeNode>[] nodes = rapqEngine.getNodes(new_CWVs);
                        for (int i = 1; i < size; i++) {
                            node_send_count_msg[1] += nodes[i].size();
                            Send_nodes[i].addAll(nodes[i]);
                        }
                        SendNodesAmongWorkers(rank, size, Send_nodes);
                        new_CWVs.forEach((mapKey, integerLinkedListMap) -> {
                            integerLinkedListMap.forEach((integer, longs) -> {
                                Map<Integer, LinkedList<Long>> worker_ts = CWVs_latest.getOrDefault(mapKey, new HashMap<>());
                                LinkedList<Long> current_ts = worker_ts.getOrDefault(integer, new LinkedList<>());
                                if (!current_ts.isEmpty()) {
                                    long last_send_time = current_ts.getLast();
                                    for (Long ts : longs) {
                                        if (ts > last_send_time)
                                            current_ts.add(ts);
                                    }
                                }
                                else
                                    current_ts.addAll(longs);
                                worker_ts.put(integer, current_ts);
                                CWVs_latest.put(mapKey, worker_ts);
                            });
                        });
                        processing_ts[1] += System.nanoTime() - begin;
                        received_msg[1]+=new_CWVs.size();
                    }
                }
                else if (!CWVs_queue.isEmpty()) {
                    HashMap<Hasher.MapKey<Integer>, Map<Integer, LinkedList<Long>>> new_CWVs = CWVs_queue.poll();
                    begin = System.nanoTime();


                    Collection<AbstractTreeNode>[] nodes = rapqEngine.getNodes(new_CWVs);
                    for (int i = 1; i < size; i++) {
                        node_send_count_msg[1] += nodes[i].size();
                        Send_nodes[i].addAll(nodes[i]);
                    }
                    SendNodesAmongWorkers(rank, size, Send_nodes);

                    new_CWVs.forEach((mapKey, integerLinkedListMap) -> {
                        integerLinkedListMap.forEach((integer, longs) -> {
                            Map<Integer, LinkedList<Long>> worker_ts = CWVs_latest.getOrDefault(mapKey, new HashMap<>());
                            LinkedList<Long> current_ts = worker_ts.getOrDefault(integer, new LinkedList<>());
                            if (!current_ts.isEmpty()) {
                                long last_send_time = current_ts.getLast();
                                for (Long ts : longs) {
                                    if (ts > last_send_time)
                                        current_ts.add(ts);
                                }
                            }
                            else
                                current_ts.addAll(longs);
                            worker_ts.put(integer, current_ts);
                            CWVs_latest.put(mapKey, worker_ts);
                        });
                    });
                    processing_ts[1] += System.nanoTime() - begin;
                    received_msg[1]+=new_CWVs.size();
                }
                else if (!message_status.isEmpty()){
                    Status status = message_status.poll();
                    switch (Objects.requireNonNull(status).tag) {
                        case Constants.EXPIRATION_REQUEST:
                            begin = System.nanoTime();
                            node_queue.poll();
                            workers_Intracomm.Barrier();
                            while (MPI.COMM_WORLD.Iprobe(MPI.ANY_SOURCE, MPI.ANY_TAG) != null){
                                TimeUnit.SECONDS.sleep(0);
                            }
                            MPI.COMM_WORLD.Isend(new long[]{LE_time}, 0, 1, MPI.LONG, 0, Constants.EXPIRATION_TIME);
                            ifCountLETime = false;
                            LE_time = Long.MAX_VALUE;
                            processing_ts[3] += System.nanoTime() - begin;
                            received_msg[3]++;
                            break;
                        case Constants.EXPIRATION_TIME:

                            break;
                        case Constants.FINISH:
                            if (!node_queue.isEmpty() || !tuple_queue.isEmpty() || !message_status.isEmpty() || MPI.COMM_WORLD.Iprobe(MPI.ANY_SOURCE, MPI.ANY_TAG) != null){
                                message_status.add(status);
                            } else {
                                Finish = true;
                                /* stop有问题，会出现有些边没收到就强制结束了 */
//                                receive_messages.interrupt();
                            }
                            break;
                        default:
                            System.out.println("error in receiving message : " + status.tag);
                    }
                }
                else Thread.sleep(0);
            }

//            rapqEngine.getResults().iterator().forEachRemaining(t-> {System.out.println("rank <" + rank +"> " +t.getSource() + " --> " + t.getTarget() + " label ts: " + t.getLabel_ts());});
            HashMap<String, Object> metric = new HashMap<>();
            if (test)
                logger.info("rank {} result count {} received tuples {} received nodes {} = {} + {}",
                        rank, rapqEngine.getResults().size(), receive_tuple, receive_node, node_send_count_msg[0], node_send_count_msg[1]);
            else {
                metric.put("TRD count", ((WindowedRPQ)rapqEngine).delta.treeIndex.values().size());
                metric.put("Received tuples", receive_tuple);
                metric.put("Received nodes", receive_node);
                metric.put("Send nodes with CWV", node_send_count_msg[0]);
                metric.put("Send nodes with EDGE", node_send_count_msg[1]);
            }
            rapqEngine.shutDown();
            double runTime = (System.nanoTime() - run_begin)*1.0/1000000000;
            double[] processing_ts_double = new double[5];
            double processing_ts_total = 0;
            double process_ts = 0;
            for (int i = 0; i < processing_ts.length; i++) {
                processing_ts_double[i] = processing_ts[i]*1.0/1000000000;
                processing_ts_total += processing_ts_double[i];
            }
            double[] avg_processing_ts = new double[5];
            for (int i = 0; i < processing_ts.length; i++) {
                if (received_msg[i] != 0)
                    avg_processing_ts[i] = processing_ts[i]*1.0/1000000 / received_msg[i];
            }
            if (test)
                logger.info(rank + "\n run time: {} processing time (s) {} total {} \n received msg count {} avg (ms) {}", runTime,
                        Arrays.toString(processing_ts_double), processing_ts_total, Arrays.toString(received_msg),  Arrays.toString(avg_processing_ts));
            else {
                metric.put("Processing time of each part", Arrays.toString(processing_ts_double));
                metric.put("Processing time (s) (total)", processing_ts_total);
                metric.put("Received nodes (detail)", Arrays.toString(received_msg));
                metric.put("Processing time (ms) of each part (average)", Arrays.toString(avg_processing_ts));
            }

            MPI.COMM_WORLD.Barrier();
            if (test)
                executor.shutdown();
            receive_messages.stop();
            if (!tuple_queue.isEmpty() || !node_queue.isEmpty() || !CWVs_queue.isEmpty())
                System.out.println("Error");

            MPI.COMM_WORLD.Send(node_send_count_msg, 0, node_send_count_msg.length, MPI.INT, 0, Constants.FINISH);

            MPI.COMM_WORLD.Send(new HashMap[]{metric}, 0, 1, MPI.OBJECT, 0, Constants.METRIC);
        }
        if (test)
            System.out.println("rank " + rank + " finalize! ");
        MPI.Finalize();
    }

    private static void expireForMaster(long minTs, HashMultimap<String, InputTuple<Integer, Integer, String>>[] label2tuples,
                                        HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> from_vertex2workers_and_times,
                                        HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> to_vertex2workers_and_times,
                                        HashMap<Hasher.MapKey<Integer>, HashMap<Hasher.MapKey<Integer>, Long>>[] node_PLS_table){
        if (minTs <= 0)
            return;

        /* maintain degree */
        for (int i = 1; i < label2tuples.length; i++) {
            Set<String> labels = new HashSet<>(label2tuples[i].keySet());
            for (String label : labels) {
                Iterator<InputTuple<Integer, Integer, String>> iterator = label2tuples[i].get(label).iterator();
                while (iterator.hasNext()){
                    InputTuple<Integer, Integer, String> inputTuple = iterator.next();
                    if (inputTuple.getTimestamp() <= minTs){
                        partial_degree.get(inputTuple.getSource())[i]--;
                        partial_degree.get(inputTuple.getTarget())[i]--;
                    }
                }
            }
        }

        int size = label2tuples.length;

        /* maintain node_PLS_table */
        for (int i = 1; i < size; i++) {
            Collection<HashMap<Hasher.MapKey<Integer>, Long>> key_ts_map_list = node_PLS_table[i].values();
            key_ts_map_list.forEach(mapKeyLongHashMap -> {
                mapKeyLongHashMap.values().removeIf(e -> e <= minTs);
            });
            key_ts_map_list.removeIf(HashMap::isEmpty);
        }

        for (int i = 1; i < label2tuples.length; i++) {
            Set<String> labels = new HashSet<>(label2tuples[i].keySet());
            for (String label : labels) {
                label2tuples[i].get(label).removeIf(e -> e.getTimestamp() <= minTs);
            }
        }
        Set<Hasher.MapKey<Integer>> vertices = new HashSet<>(from_vertex2workers_and_times.keySet());
        for (Hasher.MapKey<Integer> vertex : vertices) {
            LinkedList<Long>[] ts = from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(vertex.X, vertex.Y));
            boolean ifRemove = true;
            for (int i = 1; i < size; i++) {
                if (ts[i] != null) {
                    ts[i].removeIf(e -> e <= minTs);
                    if (!ts[i].isEmpty())
                        ifRemove = false;
                }
            }
            if (ifRemove)
                from_vertex2workers_and_times.remove(Hasher.getThreadLocalTreeNodePairKey(vertex.X, vertex.Y));
        }

        vertices = new HashSet<>(to_vertex2workers_and_times.keySet());
        for (Hasher.MapKey<Integer> vertex : vertices) {
            LinkedList<Long>[] ts = to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(vertex.X, vertex.Y));
            boolean ifRemove = true;
            for (int i = 1; i < size; i++) {
                if (ts[i] != null){
                    ts[i].removeIf(e -> e <= minTs);
                    if (!ts[i].isEmpty())
                        ifRemove = false;
                }
            }
            if (ifRemove)
                to_vertex2workers_and_times.remove(Hasher.getThreadLocalTreeNodePairKey(vertex.X, vertex.Y));
        }
    }

    private static void SendNodesAmongWorkers(int rank, int size, ArrayList<AbstractTreeNode>[] send_nodes) {
        for (int j = 1; j < size; j++) {
            if (j != rank && !send_nodes[j].isEmpty()) {
                AbstractTreeNode[] abstractTreeNodes = send_nodes[j].toArray(new AbstractTreeNode[0]);
                MPI.COMM_WORLD.Isend(abstractTreeNodes, 0, abstractTreeNodes.length, MPI.OBJECT, j, Constants.NODES);
                send_nodes[j].clear();
            }
        }
    }

    private static int QGPartition(HashMultimap<String, InputTuple<Integer, Integer, String>>[] saved_tuples, InputTuple<Integer, Integer, String> inputTuple,
                                   int k_hop, HashMap<String, HashMap<Integer, String[]>> PLS, long windowSize,
                                   int[] load_count, double Lambda){
        int size = saved_tuples.length;
        int[] G_i = new int[size];
        int source = inputTuple.getSource();
        int target = inputTuple.getTarget();
        String label = inputTuple.getLabel();
        long ts = inputTuple.getTimestamp();

        LinkedList<Integer> integers = new LinkedList<>();
        integers.add(source);
        if (PLS.containsKey(label)) {
            for (int i = 1; i < size; i++) {// worker i

                /* root node, 向后遍历 */
                if (PLS.get(label).containsKey(-1)) {
                    String[] labels = PLS.get(label).get(-1);
                    for (int k = 0; k < labels.length; k++) { // k label
                        String target_label = labels[k];
                        Set<InputTuple<Integer, Integer, String>> tuples_with_label = saved_tuples[i].get(target_label);
                        for (InputTuple<Integer, Integer, String> tuple : tuples_with_label) {
                            if (Objects.equals(tuple.getSource(), target)) {
                                G_i[i]++;
                            }
                        }
                    }
                }

                ArrayList<Integer> find_vertices = new ArrayList<>();
                int find_vertices_array_index = 0;
                find_vertices.add(source);
                for (int j = 0; j < k_hop; j++) {  // hop j
                    String[] labels = PLS.get(label).get(j);
                    if (labels != null) {
                        for (String target_label : labels) { // k label
                            Set<InputTuple<Integer, Integer, String>> tuples_with_label = saved_tuples[i].get(target_label);
                            for (InputTuple<Integer, Integer, String> tuple : tuples_with_label) {
                                int vertices_len = find_vertices.size();
                                while (find_vertices_array_index < vertices_len) {
                                    if (Objects.equals(tuple.getTarget(), find_vertices.get(find_vertices_array_index))) {
                                        find_vertices.add(tuple.getSource());
                                        G_i[i]++;
                                    }
                                    find_vertices_array_index++;
                                }
                            }
                        }
                    }
                }
            }
        }

        int target_worker = -1;
        int min = 0;
        for (int i = 1; i < size; i++) {
            if (min < G_i[i]){
                min = G_i[i];
                target_worker = i;
            }
        }

        if (min == 0){
            min = Integer.MAX_VALUE;
            for (int i = 1; i < size; i++) {
                if (min > load_count[i]){
                    min = load_count[i];
                    target_worker = i;
                }
            }
        }

//        System.out.println(target_worker);
//        double min = Double.MAX_VALUE;
//        int target_worker = -1;
//
//        for (int i = 1; i < size; i++) {
//            if (min > Lambda * load_count[i] - G_i[i]) {
//                min = G_i[i];
//                target_worker = i;
//            }
//        }
        return target_worker;
    }

    public static int Greedy(int sourceNode, int targetNode, int size,
                             HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> from_vertex2workers_and_times,
                             HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> to_vertex2workers_and_times,
                             int[] load_count, List<Map.Entry<Integer, Integer>> transitionList){

        int partitionId = -1;
        Set<Integer> servers_source = new HashSet<>();
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
//            int targetState = transition.getValue();
            if (from_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))){
                for (int i = 0; i < size; i++) {
                    if (from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i] != null &&
                            !from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i].isEmpty()){
                        servers_source.add(i);
                    }
                }
            }
            if (to_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))){
                for (int i = 0; i < size; i++) {
                    if (to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i] != null &&
                            !to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i].isEmpty()){
                        servers_source.add(i);
                    }
                }
            }
        }

        Set<Integer> prepar_server_source = new HashSet<>(servers_source);
        Set<Integer> servers_target = new HashSet<>();
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int targetState = transition.getValue();
            if (from_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))) {
                for (int i = 0; i < size; i++) {
                    if (from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i] != null &&
                            !from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i].isEmpty()) {
                        servers_target.add(i);
                    }
                }
            }
            if (to_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))) {
                for (int i = 0; i < size; i++) {
                    if (to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i] != null &&
                            !to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i].isEmpty()) {
                        servers_target.add(i);
                    }
                }
            }
        }

        if (servers_source.isEmpty() && !servers_target.isEmpty()){
            int flag = Integer.MAX_VALUE;
            for (int i : servers_target) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        } else if (!servers_source.isEmpty() && servers_target.isEmpty()){
            int flag = Integer.MAX_VALUE;
            for (int i :
                    servers_source) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        } else if (servers_source.isEmpty()){
            int flag = Integer.MAX_VALUE;
            for (int i = 1; i < size; i++) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        }

        servers_source.retainAll(servers_target);
        boolean join = servers_source.size() > 0;
        if (join){
            int flag = Integer.MAX_VALUE;
            for (int i :
                    servers_source) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        }

        servers_source = prepar_server_source;
        servers_source.removeAll(servers_target);
        servers_target.addAll(servers_source);
        if (servers_target.size() > 0){
            int flag = Integer.MAX_VALUE;
            for (int i :
                    servers_target) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        }
        return partitionId;
    }
    public static int GreedyInSpecificServers(int sourceNode, int targetNode, Set<Integer> servers,
                             HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> from_vertex2workers_and_times,
                             HashMap<Hasher.MapKey<Integer>, LinkedList<Long>[]> to_vertex2workers_and_times,
                             int[] load_count, List<Map.Entry<Integer, Integer>> transitionList){

        int partitionId = -1;
        Set<Integer> servers_source = new HashSet<>();
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int sourceState = transition.getKey();
//            int targetState = transition.getValue();
            if (from_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))){
                for (Integer i : servers) {
                    if (from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i] != null &&
                            !from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i].isEmpty()){
                        servers_source.add(i);
                    }
                }
            }
            if (to_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))){
                for (Integer i : servers) {
                    if (to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i] != null &&
                            !to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(sourceNode, sourceState))[i].isEmpty()){
                        servers_source.add(i);
                    }
                }
            }
        }

        Set<Integer> prepar_server_source = new HashSet<>(servers_source);
        Set<Integer> servers_target = new HashSet<>();
        for (Map.Entry<Integer, Integer> transition : transitionList) {
            int targetState = transition.getValue();
            if (from_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))) {
                for (Integer i : servers) {
                    if (from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i] != null &&
                            !from_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i].isEmpty()) {
                        servers_target.add(i);
                    }
                }
            }
            if (to_vertex2workers_and_times.containsKey(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))) {
                for (Integer i : servers) {
                    if (to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i] != null &&
                            !to_vertex2workers_and_times.get(Hasher.getThreadLocalTreeNodePairKey(targetNode, targetState))[i].isEmpty()) {
                        servers_target.add(i);
                    }
                }
            }
        }

        if (servers_source.isEmpty() && !servers_target.isEmpty()){
            int flag = Integer.MAX_VALUE;
            for (int i : servers_target) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        } else if (!servers_source.isEmpty() && servers_target.isEmpty()){
            int flag = Integer.MAX_VALUE;
            for (int i :
                    servers_source) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        } else if (servers_source.isEmpty()){
            int flag = Integer.MAX_VALUE;
            for (Integer i : servers) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        }

        servers_source.retainAll(servers_target);
        boolean join = servers_source.size() > 0;
        if (join){
            int flag = Integer.MAX_VALUE;
            for (int i :
                    servers_source) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        }

        servers_source = prepar_server_source;
        servers_source.removeAll(servers_target);
        servers_target.addAll(servers_source);
        if (servers_target.size() > 0){
            int flag = Integer.MAX_VALUE;
            for (int i :
                    servers_target) {
                if (load_count[i] < flag){
                    flag = load_count[i];
                    partitionId = i;
                }
            }
            return partitionId;
        }
        return partitionId;
    }

    public static int getDegree(int node, int k){
        if (partial_degree.containsKey(node)) {
            int count = 0;
            for (int i = 0; i < k; i++) {
                count += partial_degree.get(node)[i];
            }
            return count;
        }
        return 0;
    }
    public static int hdrf(int sourceNode, int targetNode, int k, int[] load_count){
        double a = 1;
        double c = 10;
        int max_load = Integer.MIN_VALUE;
        int min_load = Integer.MAX_VALUE;
        for (int i = 1; i < k; i++) {
            if (load_count[i] > max_load)
                max_load = load_count[i];
            if (load_count[i] < min_load)
                min_load = load_count[i];
        }
        double max_HDRF = Integer.MIN_VALUE;
        int alloc_id = 0;
        int sourceNode_degree = getDegree(sourceNode, k);
        int targetNode_degree = getDegree(targetNode, k);
        int theta_source = 0;
        if (sourceNode_degree != 0 || targetNode_degree != 0){
            theta_source = sourceNode_degree / (sourceNode_degree + targetNode_degree);
        }
        int theta_target = 1 - theta_source;
        for (int i = 1; i < k; i++) {
            double C_BAL = a * (max_load - load_count[i]) / (c + max_load - min_load);
            double C_REP = 0;

            int[] workers = partial_degree.get(targetNode);
            Set<Integer> source_node_servers = new HashSet<>();
            if (partial_degree.containsKey(targetNode)) {
                for (int j = 0; j < workers.length; j++) {
                    if (workers[j] > 0)
                        source_node_servers.add(j);
                }
            }

            Set<Integer> target_node_servers = new HashSet<>();
            if (partial_degree.containsKey(targetNode)) {
                workers = partial_degree.get(targetNode);
                for (int j = 0; j < workers.length; j++) {
                    if (workers[j] > 0)
                        target_node_servers.add(j);
                }
            }
            if (source_node_servers.contains(i))
                C_REP += 2 - theta_source;
            if (target_node_servers.contains(i))
                C_REP += 2 - theta_target;
            if (max_HDRF < C_BAL + C_REP){
                max_HDRF = C_BAL + C_REP;
                alloc_id = i;
            }
        }
        return alloc_id;
    }
    public static int DBH(int v_1, int v_2, int k){
        int delta_1 = 0;
        int delta_2 = 0;
        delta_1 += getDegree(v_1, k);
        delta_2 += getDegree(v_2, k);

        if (delta_1 < delta_2)
            return HASH(v_1, k) % (k-1) +1;
        else
            return HASH(v_2, k) % (k-1) +1;
    }
    public static int HASH(int vertex, int k){
        return vertex;
    }

    /* only measure the situation of k = 4 */
    public static int Grid(int sourceNode, int targetNode, int k, int[] load_count){
        int target = 0;
        k = 5;
        int hash_v1 = HASH(sourceNode, k) % (k-1) +1;
        int hash_v2 = HASH(targetNode, k) % (k-1) +1;

        Set<Integer> candidate_worker = new HashSet<>();
        candidate_worker.add(hash_v1);
        candidate_worker.add(hash_v2);
        if (hash_v1 == 1 || hash_v1 == 4 || hash_v2 == 1 || hash_v2 == 4){
            candidate_worker.add(2);candidate_worker.add(3);
        }
        if (hash_v1 == 2 || hash_v1 == 3 || hash_v2 == 2 || hash_v2 == 3){
            candidate_worker.add(1);candidate_worker.add(4);
        }

        int min_load = Integer.MAX_VALUE;
        for (Integer i : candidate_worker) {
            if (load_count[i] < min_load) {
                min_load = load_count[i];
                target = i;
            }
        }
        return target;
    }

}



