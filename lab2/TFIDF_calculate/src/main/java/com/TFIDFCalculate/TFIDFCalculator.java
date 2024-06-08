package com.TFIDFCalculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFCalculator {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TFIDF Calculate");
            job.setJarByClass(TFIDFCalculator.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(MyTextOutputFormat.class); // Use customized output format
            job.setMapperClass(TFIDFCalculateMapper.class);
            job.setReducerClass(TFIDFCalculateReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            TextInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
