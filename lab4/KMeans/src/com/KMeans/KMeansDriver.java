package com.KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class KMeansDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String datasetPath = args[0] + "/dataset.data";
        String initCenterPath = args[0] + "/initial_centers.data";
        String outputPath = args[1];

        boolean converge = false;
        int round = 0;

        while (!converge) {
            String tmpCenterPath = (round == 0 ? initCenterPath : outputPath + "_tmp/" + (round - 1) + "/part-r-00000");
            String tmpOutputPath = outputPath + "_tmp/" + round;

            Job job_iter = Job.getInstance(conf, "KMeans - Iteration " + round);

            job_iter.addCacheFile(new Path(tmpCenterPath).toUri());

            job_iter.setJarByClass(KMeansDriver.class);
            job_iter.setMapperClass(KMeansMapper.class);
            job_iter.setReducerClass(KMeansReducer.class);
            job_iter.setCombinerClass(KMeansCombiner.class);
            job_iter.setMapOutputKeyClass(IntWritable.class);
            job_iter.setMapOutputValueClass(PointFreqPair.class);
            job_iter.setOutputKeyClass(NullWritable.class);
            job_iter.setOutputValueClass(Text.class);
            job_iter.setInputFormatClass(KeyValueTextInputFormat.class);
            job_iter.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ": ");
            TextInputFormat.addInputPath(job_iter, new Path(datasetPath));
            FileOutputFormat.setOutputPath(job_iter, new Path(tmpOutputPath));

            job_iter.waitForCompletion(true);

            converge = checkConverge(FileSystem.get(conf), new Path(tmpOutputPath + "/part-r-00000"));
            round++;
        }

        Job job_outcome = Job.getInstance(conf, "KMeans - Outcome");

        String tmpOutputPath = outputPath + "_tmp/" + (round - 1);

        job_outcome.setJarByClass(KMeansDriver.class);
        job_outcome.setMapperClass(OutcomeMapper.class);
        job_outcome.setReducerClass(OutcomeReducer.class);
        job_outcome.setMapOutputKeyClass(IntWritable.class);
        job_outcome.setMapOutputValueClass(Text.class);
        job_outcome.setOutputKeyClass(IntWritable.class);
        job_outcome.setOutputValueClass(Text.class);
        job_outcome.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job_outcome, new Path(tmpOutputPath + "/part-r-00000"));
        FileOutputFormat.setOutputPath(job_outcome, new Path(outputPath));

        job_outcome.waitForCompletion(true);
    }

    private static boolean checkConverge(FileSystem fs, Path tmpOutput) throws Exception, IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(tmpOutput)))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(":");
                if (parts.length == 3 && parts[2].trim().equals("1")) {
                    return false;
                }
            }
        }
        return true;
    }
}
