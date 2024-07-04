package com.TriangleCount;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeMapper extends Mapper<Object, Text, Text, Edge> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("[\\s,]+");

        // get an edge u -> v
        String u = parts[0];
        String v = parts[1];

        // emit <u, <true, v>>
        context.write(new Text(u), new Edge(true, v));

        // emit <v, <false, u>
        context.write(new Text(v), new Edge(false, u));
    }
}
