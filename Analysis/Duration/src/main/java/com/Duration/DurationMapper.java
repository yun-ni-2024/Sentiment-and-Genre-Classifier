package com.Duration;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DurationMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        double duration = Double.parseDouble(parts[10]);

        int cluster = getCluster(duration);

        context.write(new IntWritable(cluster), new IntWritable(1));
    }

    private static int getCluster(double duration) {
        if (duration <= 60) {
            return 0;
        } else if (duration <= 120) {
            return 1;
        } else if (duration <= 180) {
            return 2;
        } else if (duration <= 240) {
            return 3;
        } else if (duration <= 300) {
            return 4;
        } else if (duration <= 360) {
            return 5;
        } else if (duration <= 420) {
            return 6;
        } else if (duration <= 480) {
            return 7;
        } else if (duration <= 540) {
            return 8;
        } else {
            return 9;
        }
    }
}
