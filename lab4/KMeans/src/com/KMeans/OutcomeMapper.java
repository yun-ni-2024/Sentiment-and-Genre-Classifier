package com.KMeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OutcomeMapper extends Mapper<Object, Text, IntWritable, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(":");
        if (parts.length == 3) {
            int index = Integer.parseInt(parts[0].trim());
            String point = parts[1].trim();

            context.write(new IntWritable(index), new Text(point));
        }
    }
}
