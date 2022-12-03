package cn.fudan.cs.drpq.runtime;

import cn.fudan.cs.drpq.input.InputTuple;
import cn.fudan.cs.drpq.input.SimpleTextStreamWithExplicitDeletions;
import cn.fudan.cs.drpq.input.TextFileStream;
import cn.fudan.cs.drpq.stree.data.ManualQueryAutomata;
import cn.fudan.cs.drpq.stree.data.arbitrary.SpanningTreeRAPQ;
import cn.fudan.cs.drpq.stree.data.arbitrary.TreeNodeRAPQ;
import cn.fudan.cs.drpq.stree.engine.RPQEngine;
import cn.fudan.cs.drpq.stree.engine.WindowedRPQ;
import cn.fudan.cs.drpq.stree.util.Semantics;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SOQueryRunner {

//    static String filename = "src/main/resources/diamondgraph.txt";
//    static String filename = "src/main/resources/SO-aviation.stackexchange.com.txt";
//    static String filename = "src/main/resources/SO-codereview.meta.stackexchange.com.txt";
//    static InputStream inputStream = SOQueryRunner.class.getClassLoader().getResourceAsStream("SO-es.stackoverflow.com.txt");
    static String filename = "diamondgraph.txt";
//    static String filename = "Yago-test.txt";
    static final Logger logger = LoggerFactory.getLogger(SOQueryRunner.class);
    public static void main(String[] args) {
        int numofStates = 3;
        long begin = System.nanoTime();
        ManualQueryAutomata<String> query = new ManualQueryAutomata<String>(numofStates);
        query.addFinalState(numofStates-1);
        query.addTransition(0, "a", 1);
        query.addTransition(1, "b", 2);
        query.addTransition(2, "a", 1);


        RPQEngine<String> rapqEngine = new WindowedRPQ<String, SpanningTreeRAPQ<Integer>, TreeNodeRAPQ<Integer>>(query, 100, 10, 1,
                10, Semantics.ARBITRARY, 1, 1);
        MetricRegistry metricRegistry = new MetricRegistry();
        rapqEngine.addMetricRegistry(metricRegistry);

        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);


        TextFileStream stream = new SimpleTextStreamWithExplicitDeletions();
        stream.open(filename);
        InputTuple<Integer, Integer, String> input = stream.next();

        while (input != null) {
//            rapqEngine.processEdge(input, null);
            input = stream.next();
        }

//        rapqEngine.getResults().iterator().forEachRemaining(t-> {System.out.println(t.getSource() + " --> " + t.getTarget() + " " + !t.isDeletion());});
        System.out.println(rapqEngine.getResults().size());
        rapqEngine.shutDown();

        stream.close();

        reporter.stop();
        double runTime = (System.nanoTime() - begin)*1.0/1000000000;
        logger.info("run time: {}", runTime);
    }
    
}
