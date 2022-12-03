package cn.fudan.cs.drpq;

import org.apache.jena.base.Sys;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;

public class Test {
    public static void main(String[]args) {

        File file = new File("./result_clock_ts_2.txt");
        BufferedReader reader = null;
        try {
//            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            reader.readLine();
            reader.readLine();
            HashSet<String> result1 = new HashSet<>();
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                String[] in = tempString.split(" ");
                if (result1.contains(in[0] + " " + in[2]))
                    System.out.println("double: " + in[0] + " " + in[2]);
                else
                    result1.add(in[0] + "->" + in[2]);
            }
            reader.close();
            file = new File("./result_clock_ts_1.txt");
            reader = new BufferedReader(new FileReader(file));
            HashSet<String> result2 = new HashSet<>();
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                String[] in = tempString.split(" ");
                if (result2.contains(in[0] + " " + in[2]))
                    System.out.println("double: " + in[0] + " " + in[2]);
                else
                    result2.add(in[0] + "->" + in[2]);
            }
            HashSet<String> join_result = new HashSet<>();
            join_result.clear();
            join_result.addAll(result1);
            join_result.addAll(result2);
            HashSet<String> dist_result = new HashSet<>();
            dist_result.clear();
            dist_result.addAll(result1);
            dist_result.retainAll(result2);

            join_result.removeAll(dist_result);
            System.out.println("diff size:" + join_result.size());
            reader.close();
            for (String s: join_result)
                System.out.println(s);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
