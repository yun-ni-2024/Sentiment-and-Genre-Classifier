package com.TriangleCountAnd;

import java.util.HashSet;
import java.util.Set;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // current vertex
        String u = key.toString();

        // outgoing and incoming edges
        Set<String> outEdge = new HashSet<>();
        Set<String> inEdge = new HashSet<>();

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String dir = parts[0];
            String v = parts[1];

            if (dir.equals("1")) {
                outEdge.add(v);
            } else {
                inEdge.add(v);
            }

//            context.write(new Text(u), new Text(dir + "," + v));
        }

//        context.write(new Text(u), new Text(String.join(",", outEdge) + ":" + String.join(",", inEdge)));

        // edges of u in the undirected graph
        Set<String> edge = new HashSet<>();
        for (String vertex : outEdge) {
            if (inEdge.contains(vertex)) {
                edge.add(vertex);
            }
        }
//        outEdge.retainAll(inEdge);


//        if (!edge.isEmpty()) {
        // write <u, edge_u> to the edge file
        context.write(new Text(u), new Text(String.join(",", edge)));
//        }
    }
}
