package cn.fudan.cs.drpq;

import cn.fudan.cs.drpq.input.InputTuple;
import cn.fudan.cs.drpq.input.SimpleTextStreamWithExplicitDeletions;
import cn.fudan.cs.drpq.input.TextFileStream;

import java.util.HashSet;

public class InputAnalysis {
    public static void main(String[]args){
        String filename = "diamondgraph.txt";
        TextFileStream<Integer, Integer, String> stream = new SimpleTextStreamWithExplicitDeletions();
        stream.open(filename);
        InputTuple<Integer, Integer, String> input = stream.next();
        HashSet<String> inputTuples = new HashSet<>();
        while (input != null) {
            if (inputTuples.contains(input.toString())){
                System.out.println(input);
            }else
                inputTuples.add(input.toString());
            input = stream.next();
        }
//        System.out.println(inputTuples.toString());
        stream.close();
    }
}
