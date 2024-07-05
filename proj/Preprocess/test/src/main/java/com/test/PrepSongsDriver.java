package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrepSongsDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = "proj/SongDataset/songs/A/A/A";
        String outputPath = "proj/songs";

        Job job = Job.getInstance(conf, "Preprocess songs");

        job.setJarByClass(PrepSongsDriver.class);
        job.setMapperClass(PrepSongsMapper.class);
        job.setReducerClass(PrepSongsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setInputFormatClass(FileInputFormat.class);
//        FileInputFormat.setInputDirRecursive(job, true);  // Enable recursive input path traversal
        TextInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
