package com.RemoveRedundant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RemoveRedundantDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

//        Job job1 = Job.getInstance(conf, "Remove Redundant Lyrics");
//        job1.addCacheFile(new Path(inputPath + "/songs.txt").toUri());
//        job1.setJarByClass(RemoveRedundantDriver.class);
//        job1.setMapperClass(RemoveRedundantLyricsMapper.class);
//        job1.setReducerClass(RemoveRedundantLyricsReducer.class);
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(Text.class);
//        job1.setOutputKeyClass(NullWritable.class);
//        job1.setOutputValueClass(Text.class);
//        TextInputFormat.addInputPath(job1, new Path(inputPath + "/lyrics.txt"));
//        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/lyrics_txt"));
//        job1.waitForCompletion(true);

//        Job job2 = Job.getInstance(conf, "Remove Redundant Genres");
//        job2.addCacheFile(new Path(inputPath + "/songs.txt").toUri());
//        job2.setJarByClass(RemoveRedundantDriver.class);
//        job2.setMapperClass(RemoveRedundantGenresMapper.class);
//        job2.setReducerClass(RemoveRedundantGenresReducer.class);
//        job2.setMapOutputKeyClass(Text.class);
//        job2.setMapOutputValueClass(Text.class);
//        job2.setOutputKeyClass(NullWritable.class);
//        job2.setOutputValueClass(Text.class);
//        TextInputFormat.addInputPath(job2, new Path(inputPath + "/genres.txt"));
//        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/genres_txt"));
//        job2.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "Remove Redundant Users");
        job2.addCacheFile(new Path(inputPath + "/songs.txt").toUri());
        job2.setJarByClass(RemoveRedundantDriver.class);
        job2.setMapperClass(RemoveRedundantUsersMapper.class);
        job2.setReducerClass(RemoveRedundantUsersReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job2, new Path(inputPath + "/users.txt"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/users_txt"));
        job2.waitForCompletion(true);
    }
}
