package com.TriangleCountOr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TriangleCountMapper extends Mapper<Object, Text, Text, EdgeList> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\s+");
        String u = parts[0];
        String[] edge = parts[1].split(",");

        for (String v : edge) {
            context.write(new Text(v), new EdgeList(u, new ArrayList<>(Arrays.asList(edge))));
        }
    }
}
