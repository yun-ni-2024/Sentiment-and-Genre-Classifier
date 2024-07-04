package com.TriangleCountOr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("[\\s,]+");
        String u = parts[0];
        String v = parts[1];

        context.write(new Text(u), new Text(v));
        context.write(new Text(v), new Text(u));
    }
}
