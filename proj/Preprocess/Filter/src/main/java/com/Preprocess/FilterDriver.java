package com.Preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class FilterDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

//        Job job1 = Job.getInstance(conf, "Filter Users");
//        job1.addCacheFile(new Path(inputPath + "/songs.txt").toUri());
//        job1.setJarByClass(FilterDriver.class);
//        job1.setMapperClass(FilterUsersMapper.class);
//        job1.setReducerClass(FilterUsersReducer.class);
//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(Text.class);
//        job1.setOutputKeyClass(NullWritable.class);
//        job1.setOutputValueClass(Text.class);
//        TextInputFormat.addInputPath(job1, new Path(inputPath + "/users.txt"));
//        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/tmp_Filter_Users"));
//        job1.waitForCompletion(true);
//
//        copyFile(outputPath + "/tmp_Filter_Users/part-r-00000", outputPath + "/filtered_users.txt");
//        deletePath(outputPath + "/tmp_Filter_Users", true);

//        Job job2 = Job.getInstance(conf, "Filter Lyrics");
//        job2.addCacheFile(new Path(inputPath + "/songs.txt").toUri());
//        job2.addCacheFile(new Path(inputPath + "/genres.txt").toUri());
//        job2.setJarByClass(FilterDriver.class);
//        job2.setMapperClass(FilterLyricsMapper.class);
//        job2.setReducerClass(FilterLyricsReducer.class);
//        job2.setMapOutputKeyClass(Text.class);
//        job2.setMapOutputValueClass(Text.class);
//        job2.setOutputKeyClass(NullWritable.class);
//        job2.setOutputValueClass(Text.class);
//        TextInputFormat.addInputPath(job2, new Path(inputPath + "/lyrics.txt"));
//        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/tmp_Filter_Lyrics"));
//        job2.waitForCompletion(true);
//
//        copyFile(outputPath + "/tmp_Filter_Lyrics/part-r-00000", outputPath + "/filtered_lyrics.txt");
//        deletePath(outputPath + "/tmp_Filter_Lyrics", true);

        Job job3 = Job.getInstance(conf, "Filter Genres");
        job3.addCacheFile(new Path(inputPath + "/songs.txt").toUri());
        job3.addCacheFile(new Path(inputPath + "/lyrics.txt").toUri());
        job3.setJarByClass(FilterDriver.class);
        job3.setMapperClass(FilterGenresMapper.class);
        job3.setReducerClass(FilterGenresReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job3, new Path(inputPath + "/genres.txt"));
        FileOutputFormat.setOutputPath(job3, new Path(outputPath + "/tmp_Filter_Genres"));
        job3.waitForCompletion(true);

        copyFile(outputPath + "/tmp_Filter_Genres/part-r-00000", outputPath + "/filtered_genres.txt");
        deletePath(outputPath + "/tmp_Filter_Genres", true);

//        Job job3 = Job.getInstance(conf, "Filter Songs");
//        job3.addCacheFile(new Path(inputPath + "/lyrics.txt").toUri());
//        job3.addCacheFile(new Path(inputPath + "/genres.txt").toUri());
//        job3.addCacheFile(new Path(inputPath + "/users.txt").toUri());
//        job3.setJarByClass(FilterDriver.class);
//        job3.setMapperClass(FilterSongsMapper.class);
//        job3.setReducerClass(FilterSongsReducer.class);
//        job3.setMapOutputKeyClass(Text.class);
//        job3.setMapOutputValueClass(Text.class);
//        job3.setOutputKeyClass(NullWritable.class);
//        job3.setOutputValueClass(Text.class);
//        TextInputFormat.addInputPath(job3, new Path(inputPath + "/songs.txt"));
//        FileOutputFormat.setOutputPath(job3, new Path(outputPath + "/tmp_Filter_Songs"));
//        job3.waitForCompletion(true);
//
//        copyFile(outputPath + "/tmp_Filter_Songs/part-r-00000", outputPath + "/filtered_songs.txt");
//        deletePath(outputPath + "/tmp_Filter_Songs", true);
    }

    public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    public static void copyFile(String from_path, String to_path) throws IOException {
        Path path_from = new Path(from_path);
        Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);
        Text line = new Text();
        while(lineReader.readLine(line) > 0) {
            String str = line.toString() + "\n";
            outputStream.write(str.getBytes());
        }
        lineReader.close();
        outputStream.close();
    }
}
