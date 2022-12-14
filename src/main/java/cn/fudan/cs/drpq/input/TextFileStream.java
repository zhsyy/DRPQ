package cn.fudan.cs.drpq.input;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public abstract class TextFileStream<S, T, L> {

    private final char FIELD_SEPERATOR = '\t';

    private final Logger logger = LoggerFactory.getLogger(TextFileStream.class);

    protected InputStream fileStream;
    protected BufferedReader bufferedReader;

    protected String filename;

    protected ScheduledExecutorService executor;

    protected int localCounter = 0;
    protected int globalCounter = 0;
    protected int deleteCounter = 0;

    //不指定startTimestamp时，默认的startTimestamp在open文件时会设置为0：此处-1L就是表示的-1，占8个字节
    protected long startTimestamp = -1L;
    protected long lastTimestamp = Long.MIN_VALUE;

    protected Queue<String> deletionBuffer;
    protected int deletionPercentage = 0;

    protected String splitResults[];

    protected InputTuple<S, T, L> tuple = null;

    public void open(String filename) {
        this.filename = filename;
        fileStream = this.getClass().getClassLoader().getResourceAsStream(filename);
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(fileStream, StandardCharsets.UTF_8));
            String info = bufferedReader.readLine();

        }catch (Exception e){
            e.printStackTrace();
        }
//        Runnable counterRunnable = new Runnable() {
//            private int seconds = 0;
//            @Override
//            public void run() {
//                System.out.println("Second " + ++seconds + " : " + localCounter + " / " + globalCounter + " ~ avg:" + globalCounter/seconds);
//                localCounter = 0;
//            }
//        };
//
//        this.executor = Executors.newScheduledThreadPool(1);
//        this.executor.scheduleAtFixedRate(counterRunnable, 1, 1, TimeUnit.SECONDS);

        this.splitResults = new String[5];
        this.deletionBuffer = new ArrayDeque<String>();
        this.tuple = new InputTuple(null, null, null, 0);
    }

    public void open(String filename, int maxSize) {
        this.startTimestamp = 0L;
        open(filename);
    }

    public void open(String filename, int maxSize, long startTimestamp, int deletionPercentage) {
        this.startTimestamp = startTimestamp;
        open(filename);
    }

    /**
     * Parse a single line from the source and populate splitResults for the creation of the next tuple
     * @param line
     * @return total number of fields parsed from the line
     */
    protected int parseLine(String line) {
        int i = 0;
        Iterator<String> iterator = Splitter.on(getFieldSeperator()).trimResults().split(line).iterator();
        for(i = 0; iterator.hasNext() && i < 5; i++) {
            splitResults[i] = iterator.next();
        }
        return i;
    }

    /**
     * Seperator character that used to parse fields from a given line.
     * Default seperator is <code>tab</code> character. Must be overriden by implementations if a different seperator is used
     * @return the field separator, TAB by default
     */
    protected char getFieldSeperator() {
        return FIELD_SEPERATOR;
    }

    /**
     * Generate the next input tuple
     * @return
     */
    public InputTuple<S, T, L> next() {
        InputTuple<S, T, L> tuple = new InputTuple<S, T, L>(null, null ,null, 0);
        String line = null;
//        System.out.println("next line");
        //generate negative tuple if necessary
        if(!deletionBuffer.isEmpty() && ThreadLocalRandom.current().nextInt(100) < deletionPercentage) {
            line = deletionBuffer.poll();
            int i = parseLine(line);
            // only if we fully
            if (i == getRequiredNumberOfFields()) {
                setSource(tuple);
                setLabel(tuple);
                setTarget(tuple);
                setTimestamp(tuple);

//                tuple.setType(InputTuple.TupleType.DELETE);

                deleteCounter++;
                return tuple;
            }
        }

        try {
            while((line = bufferedReader.readLine()) != null) {
                int i = parseLine(line);
                // only if we fully
                if(i == getRequiredNumberOfFields()) {
                    setSource(tuple);
                    setLabel(tuple);
                    setTarget(tuple);
                    updateCurrentTimestamp();
                    setTimestamp(tuple);

//                    tuple.setType(InputTuple.TupleType.INSERT);

                    localCounter++;
                    globalCounter++;

                    // store this tuple for later deletion
                    if(ThreadLocalRandom.current().nextInt(100) < 2 * deletionPercentage) {
                        deletionBuffer.offer(line);
                    }

                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Parsing input line: {}", line, e);
            return null;
        }

        if (line == null) {
            return null;
        }

        return tuple;
    }

    /**
     * the total number of fields that needs to be parsed from a line
     * @return
     */
    protected abstract int getRequiredNumberOfFields();

    protected abstract void setSource(InputTuple<S, T, L> tuple);

    protected abstract void setTarget(InputTuple<S, T, L> tuple);

    protected abstract void setLabel(InputTuple<S, T, L> tuple);

    protected abstract void updateCurrentTimestamp();

    protected abstract void setTimestamp(InputTuple<S, T, L> tuple);

    public void close() {
//        executor.shutdown();
        try {
            bufferedReader.close();

//            fileStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public abstract void reset();

}
