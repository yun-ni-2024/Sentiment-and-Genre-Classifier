package com.TriangleCountOr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\s+");
        String ver = parts[0];
        int count = Integer.parseInt(parts[1]);

        context.write(new Text("default"), new IntWritable(count));
    }
}
