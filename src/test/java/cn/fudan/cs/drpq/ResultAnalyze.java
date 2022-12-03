package cn.fudan.cs.drpq;

import cn.fudan.cs.drpq.stree.data.ResultPair;
import cn.fudan.cs.drpq.stree.util.Hasher;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class ResultAnalyze {
    public static void main(String args[]){
        long window_size = 150000;
        HashMap<String, String> send_ts = new HashMap<>();
        try {
            FileInputStream inputStream = new FileInputStream("./send_clock_ts1.txt");
            ObjectInputStream  bufferedReader = new ObjectInputStream(inputStream);
            HashMap<Long, LinkedList<Long>> label_ts_to_clock_ts_list = (HashMap<Long, LinkedList<Long>>) bufferedReader.readObject();
            System.out.println("traverse 1 ok");
            inputStream = new FileInputStream("./result_clock_ts1.txt");
            bufferedReader = new ObjectInputStream(inputStream);
            ArrayList<ResultPair<Integer>> resultPairs = (ArrayList<ResultPair<Integer>>) bufferedReader.readObject();
            System.out.println("traverse 2 ok");

            MetricRegistry metricRegistry = new MetricRegistry();
            Histogram latency_histogram =  new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));;
            metricRegistry.register("latency-histogram", latency_histogram);
            Histogram latency_low =  new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));;
            metricRegistry.register("latency_low", latency_low);
            Histogram latency_upp =  new Histogram(new SlidingTimeWindowArrayReservoir(10, TimeUnit.MINUTES));;
            metricRegistry.register("latency_upp-histogram", latency_upp);
            final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();

            System.out.println("begin");
            int count = 0;
            for (Long aLong : label_ts_to_clock_ts_list.keySet()) {
                count++;
                long send_clock = label_ts_to_clock_ts_list.get(aLong).getLast();
                HashMap<MapKey, Long> k_v_to_time_stamp = new HashMap<>();
                for (ResultPair<Integer> resultPair : resultPairs) {
                    if (resultPair.contain(aLong)) {
                        if (k_v_to_time_stamp.containsKey(new MapKey(resultPair.getSource(), resultPair.getTarget()))) {
                            if (k_v_to_time_stamp.get(new MapKey(resultPair.getSource(), resultPair.getTarget())) > resultPair.getClock_ts()) {
                                k_v_to_time_stamp.put(new MapKey(resultPair.getSource(), resultPair.getTarget()), resultPair.getClock_ts());
                            }
                        } else
                            k_v_to_time_stamp.put(new MapKey(resultPair.getSource(), resultPair.getTarget()), resultPair.getClock_ts());
                    }
                }
                for (Long value : k_v_to_time_stamp.values()) {
                    latency_histogram.update(value - send_clock);
                }
                if (count == 10)
                    break;
            }





//            for (ResultPair<Integer> resultPair : resultPairs) {
////                System.out.println(resultPair);
//                if (k_v_to_time_stamp.containsKey(new MapKey(resultPair.getSource(), resultPair.getTarget()))){
//                    long max_ts = k_v_to_time_stamp.get(new MapKey(resultPair.getSource(), resultPair.getTarget()));
//                    while (!resultPair.getUpper_ts().isEmpty() && resultPair.getUpper_ts().getFirst() <= max_ts){
//                        resultPair.getLower_ts().removeFirst();
//                        resultPair.getUpper_ts().removeFirst();
//                    }
//                    if (resultPair.getUpper_ts().isEmpty())
//                        continue;
//                }
//                k_v_to_time_stamp.put(new MapKey(resultPair.getSource(), resultPair.getTarget()), resultPair.getUpper_ts().getLast());
//                long clock = resultPair.getClock_ts();
//                while(!resultPair.getUpper_ts().isEmpty()) {
//                    long low = resultPair.getLower_ts().removeFirst();
//                    long upp = resultPair.getUpper_ts().removeFirst();
//                    for (long i = low - window_size; i < upp - window_size; i++) {
//                        if (label_ts_to_clock_ts_list.containsKey(i)) {
//                            latency_low.update(clock - label_ts_to_clock_ts_list.get(i).getLast());
//                            latency_histogram.update(clock - label_ts_to_clock_ts_list.get(i).getLast());
//                        } else if (label_ts_to_clock_ts_list.containsKey(i + window_size)) {
//                            latency_upp.update(clock - label_ts_to_clock_ts_list.get(i + window_size).getLast());
//                            latency_histogram.update(clock - label_ts_to_clock_ts_list.get(i + window_size).getLast());
//                        }
//                    }
//                }
//            }

//            String str = null;
//            while((str = bufferedReader.readLine()) != null)
//            {
//                String[] strings = str.split(" ");
//                if (strings.length == 3){
//                    send_ts.put(strings[1], strings[2]);
//                }
//            }

//            inputStream = new FileInputStream("./result_clock_ts_1.txt");
//            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
//            while((str = bufferedReader.readLine()) != null)
//            {
//                String[] strings = str.split(" ");
//                long ts = Long.parseLong(strings[2]) - Long.parseLong(send_ts.get(strings[1]));
//                latency_histogram.update(ts);
//            }
//            inputStream = new FileInputStream("./result_clock_ts_2.txt");
//            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
//            while((str = bufferedReader.readLine()) != null)
//            {
//                String[] strings = str.split(" ");
//                long ts = Long.parseLong(strings[2]) - Long.parseLong(send_ts.get(strings[1]));
//                latency_histogram.update(ts);
//            }
            //close
            inputStream.close();
            bufferedReader.close();
            reporter.report();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
    public static class MapKey implements Serializable {

        public int X;
        public int Y;

        public MapKey(int X, int Y) {
            this.X = X;
            this.Y = Y;
        }

        @Override
        public boolean equals (final Object O) {
            if (!(O instanceof Hasher.MapKey)) return false;
            if (!((Hasher.MapKey) O).X.equals(X)) return false;
            if (((Hasher.MapKey) O).Y != Y) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 31 * h + X;
            h = 31 * h + Y;
            return h;
        }
        @Override
        public String toString() {
            return "<" + X + ", " + Y + ">";
        }
    }
}
