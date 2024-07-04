package com.Classification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClassificationDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String datasetPath = args[0] + "/dataset.data";
        String centerPath = args[1] + "/part-r-00000";
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "KMeans - Classification");

        job.addCacheFile(new Path(centerPath).toUri());

        job.setJarByClass(ClassificationDriver.class);
        job.setMapperClass(ClassificationMapper.class);
        job.setReducerClass(ClassificationReducer.class);
        job.setPartitionerClass(ClassificationPartitioner.class);
        job.setMapOutputKeyClass(IndexPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(datasetPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
