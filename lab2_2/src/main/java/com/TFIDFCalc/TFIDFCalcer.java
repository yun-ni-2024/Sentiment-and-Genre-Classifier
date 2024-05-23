package com.TFIDFCalc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TFIDFCalcer {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TFIDF Calc");
            job.setJarByClass(TFIDFCalcer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(TFIDFCalcMapper.class);
//            job.setCombinerClass(TFIDFCalcCombiner.class);
            job.setReducerClass(TFIDFCalcReducer.class);
//            job.setOutputKeyClass(StringPair.class);
//            job.setOutputValueClass(DoubleWritable.class);
//            job.setOutputKeyClass(StringPair.class);
//            job.setOutputValueClass(DoubleWritable.class);
                        job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
//            job.setMapOutputKeyClass(Text.class);
////            job.setMapOutputValueClass(StringIntPair.class);
//            job.setMapOutputValueClass(Text.class);
            TextInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
