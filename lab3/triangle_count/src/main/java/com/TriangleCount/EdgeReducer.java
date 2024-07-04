package com.TriangleCount;

import java.util.HashSet;
import java.util.Set;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeReducer extends Reducer<Text, Edge, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Edge> values, Context context) throws IOException, InterruptedException {
        // current vertex
        String u = key.toString();

        // outgoing and incoming edges
        Set<String> outEdge = new HashSet<>();
        Set<String> inEdge = new HashSet<>();

        for (Edge edge : values) {
            if (edge.isOut()) {
                outEdge.add(edge.getVertex());
            } else {
                inEdge.add(edge.getVertex());
            }
        }

        // edges of u in the undirected graph
        Set<String> edge = new HashSet<>(outEdge);

        edge.retainAll(inEdge);

        if (!edge.isEmpty()) {
            // write <u, edge_u> to the edge file
            context.write(new Text(u), new Text(String.join(",", edge)));
        }
    }
}
