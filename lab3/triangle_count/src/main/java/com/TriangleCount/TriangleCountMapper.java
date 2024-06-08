package com.TriangleCount;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TriangleCountMapper extends Mapper<Object, Text, Text, StringListPair> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        HashMap<String, ArrayList<String>> vertexOut = new HashMap<>();
        HashMap<String, ArrayList<String>> vertexIn = new HashMap<>();

        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String u = itr.nextToken();
            String v = itr.nextToken();

            // add edge <u, v>
            if (vertexOut.containsKey(u)) {
                vertexOut.get(u).add(v);
            } else {
                ArrayList<String> list = new ArrayList<>();
                list.add(v);
                vertexOut.put(u, list);
            }

            // add inverted edge <v, u>
            if (vertexIn.containsKey(v)) {
                vertexIn.get(v).add(u);
            } else {
                ArrayList<String> list = new ArrayList<>();
                list.add(u);
                vertexIn.put(v, list);
            }
        }



        Iterator<Map.Entry<String, ArrayList<String>>> vertexItr = vertex.entrySet().iterator();
        while (vertexItr.hasNext()) {
            Map.Entry<String, ArrayList<String>> entry = vertexItr.next();
            String u = entry.getKey();
            ArrayList<String> list = entry.getValue();
            for (int i = 0; i < list.size(); i++) {
                String v = list.get(i);
                context.write(new Text(u), new StringListPair(u, list));
            }
        }
    }
}
