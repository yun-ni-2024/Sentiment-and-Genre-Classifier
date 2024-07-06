package com.PrepLyrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrepLyricsDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];
        String wordsPath = args[0] + "/words.txt";

        Job job = Job.getInstance(conf, "Preprocess Lyrics");

        job.addCacheFile(new Path(wordsPath).toUri());

        job.setJarByClass(PrepLyricsDriver.class);
        job.setMapperClass(PrepLyricsMapper.class);
        job.setReducerClass(PrepLyricsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.waitForCompletion(true);
    }
}
