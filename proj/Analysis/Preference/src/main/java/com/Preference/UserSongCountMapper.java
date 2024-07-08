package com.Preference;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserSongCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        String userId = parts[0];
        int count = Integer.parseInt(parts[2]);

        context.write(new Text(userId), new IntWritable(count));
    }
}
