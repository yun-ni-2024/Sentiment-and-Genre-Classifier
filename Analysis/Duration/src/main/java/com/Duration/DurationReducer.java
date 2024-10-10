package com.Duration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DurationReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int cluster = key.get();

        int totCount = 0;
        for (IntWritable value : values) {
            totCount += value.get();
        }

        String clusterStr = "none";
        if (cluster == 9) {
            clusterStr = "[541,~]";
        } else if (cluster == 0) {
            clusterStr = "[0,60]";
        } else {
            clusterStr = "[" + (cluster * 60 + 1) + "," + ((cluster + 1) * 60) + "]";
        }

        context.write(new Text(clusterStr), new IntWritable(totCount));
    }
}
