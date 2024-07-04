package com.TriangleCount;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TriangleCountReducer extends Reducer<Text, EdgeList, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<EdgeList> values, Context context) throws IOException, InterruptedException {
        String u = key.toString();

        // adjacent list of u
        Set<String> edge_u = new HashSet<>();

        // adjacent list of all vertices
        HashMap<String, Set<String>> edge = new HashMap<>();

        for (EdgeList edgeList : values) {
            String v = edgeList.getVertex();
            ArrayList<String> tmp = edgeList.getEdges();
            Set<String> edge_v = new HashSet<>();
            for (String w : tmp) {
                edge_v.add(w);
            }

            edge_u.add(v);

            // edge[v] = edge_v
            edge.put(v, edge_v);
        }

        // number of triangles related to u
        int count = 0;

        for (String v : edge_u) {
            for (String w : edge_u) {
                if (!v.equals(w) && edge.get(v).contains(w)) {
                    count++;
                }
            }
        }

        count /= 2;

        // write <u, count_u>
        context.write(new Text(u), new IntWritable(count));
    }
}