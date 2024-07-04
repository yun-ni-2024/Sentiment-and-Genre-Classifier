package com.TriangleCountOr;

import java.util.HashSet;
import java.util.Set;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String u = key.toString();
        Set<String> edge = new HashSet<>();

        for (Text value : values) {
            String v = value.toString();
            edge.add(v);
        }

        if (!edge.isEmpty()) {
            context.write(new Text(u), new Text(String.join(",", edge)));
        }
    }
}
