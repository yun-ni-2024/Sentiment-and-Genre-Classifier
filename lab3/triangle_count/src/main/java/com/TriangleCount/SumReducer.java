package com.TriangleCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            int count = value.get();
            sum += count;
        }

        // each triangle has been counted three times
        sum /= 3;

        // write <sum>
        context.write(null, new IntWritable(sum));
    }
}
