package com.TriangleCountOr;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TriangleCountReducer extends Reducer<Text, EdgeList, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<EdgeList> values, Context context) throws IOException, InterruptedException {
        String s = key.toString();

        Set<String> edge_s = new HashSet<>();
        HashMap<String, Set<String>> edge = new HashMap<>();

        for (EdgeList edgeList : values) {
            String u = edgeList.getVertex();
            ArrayList<String> tmp = edgeList.getEdges();
            Set<String> edge_u = new HashSet<>();
            for (String v : tmp) {
                edge_u.add(v);
            }

            edge_s.add(u);
            edge.put(u, edge_u);
        }

        int count = 0;

        for (String u : edge_s) {
            for (String v : edge_s) {
                if (!u.equals(v) && edge.get(u).contains(v)) {
                    count++;
                }
            }
        }

        count /= 2;

        context.write(new Text(s), new IntWritable(count));
    }
}