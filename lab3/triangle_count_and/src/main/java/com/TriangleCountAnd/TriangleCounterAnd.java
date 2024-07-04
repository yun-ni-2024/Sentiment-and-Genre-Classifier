package com.TriangleCountAnd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleCounterAnd {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            // Job 1: Generate edge lists
            Job job1 = Job.getInstance(conf, "Generate edge lists");
            job1.setJarByClass(TriangleCounterAnd.class);
            job1.setMapperClass(EdgeMapper.class);
            job1.setReducerClass(EdgeReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            TextInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));

            System.exit(job1.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
