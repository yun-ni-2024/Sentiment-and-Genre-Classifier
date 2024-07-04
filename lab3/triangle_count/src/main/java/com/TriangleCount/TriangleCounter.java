package com.TriangleCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleCounter {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            // Job 1: Generate edge lists
            Job job1 = Job.getInstance(conf, "Generate edge lists");
            job1.setJarByClass(TriangleCounter.class);
            job1.setMapperClass(EdgeMapper.class);
            job1.setReducerClass(EdgeReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Edge.class);
            TextInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path("lab3/edge"));

            if (!job1.waitForCompletion(true)) {
                System.err.println("Job 1 failed");
                System.exit(1);
            }
            System.out.println("Job 1 completed successfully");

            // Job 2: Count triangle
            Job job2 = Job.getInstance(conf, "Count triangle");
            job2.setJarByClass(TriangleCounter.class);
            job2.setMapperClass(TriangleCountMapper.class);
            job2.setReducerClass(TriangleCountReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(EdgeList.class);
            TextInputFormat.addInputPath(job2, new Path("lab3/edge"));
            FileOutputFormat.setOutputPath(job2, new Path("lab3/count"));

            if (!job2.waitForCompletion(true)) {
                System.err.println("Job 2 failed");
                System.exit(1);
            }
            System.out.println("Job 2 completed successfully");

            // Job 3: Sum up counts
            Job job3 = Job.getInstance(conf, "Sum up counts");
            job3.setJarByClass(TriangleCounter.class);
            job3.setMapperClass(SumMapper.class);
            job3.setReducerClass(SumReducer.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(IntWritable.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(IntWritable.class);
            TextInputFormat.addInputPath(job3, new Path("lab3/count"));
            FileOutputFormat.setOutputPath(job3, new Path(args[1]));

            if (!job3.waitForCompletion(true)) {
                System.err.println("Job 3 failed");
                System.exit(1);
            }
            System.out.println("Job 3 completed successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
