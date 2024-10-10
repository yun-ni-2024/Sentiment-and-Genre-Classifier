package com.Preference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class PreferenceDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

        Job job1 = Job.getInstance(conf, "User Song Count");
        job1.setJarByClass(PreferenceDriver.class);
        job1.setMapperClass(UserSongCountMapper.class);
        job1.setReducerClass(UserSongCountReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath + "/filtered_users.txt"));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/tmp_User_Song_Count"));
        job1.waitForCompletion(true);
        getHighest(outputPath + "/tmp_User_Song_Count/part-r-00000", outputPath + "/tmp_User_Song_Count/user_song_highest.txt");

        Job job2 = Job.getInstance(conf, "Artist Song Count");
        job2.addCacheFile(new Path(inputPath + "/filtered_songs.txt").toUri());
        job2.setJarByClass(PreferenceDriver.class);
        job2.setMapperClass(ArtistSongCountMapper.class);
        job2.setReducerClass(ArtistSongCountReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job2, new Path(inputPath + "/filtered_users.txt"));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/tmp_Artist_Song_Count"));
        job2.waitForCompletion(true);
        getHighest(outputPath + "/tmp_Artist_Song_Count/part-r-00000", outputPath + "/tmp_Artist_Song_Count/artist_song_highest.txt");

        combineResult(outputPath + "/tmp_User_Song_Count/user_song_highest.txt", outputPath + "/tmp_Artist_Song_Count/artist_song_highest.txt", outputPath + "/task22.txt");
        deletePath(outputPath + "/tmp_User_Song_Count", true);
        deletePath(outputPath + "/tmp_Artist_Song_Count", true);
    }

    public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    public static void getHighest(String from_path, String to_path) throws IOException {
        Path path_from = new Path(from_path);
        Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);

        Text line = new Text();
        String maxCountName = "none";
        int maxCount = 0;

        while(lineReader.readLine(line) > 0) {
            String[] parts = line.toString().split("\t");
            String name = parts[0];
            int count = Integer.parseInt(parts[1]);

            if (count > maxCount) {
                maxCountName = name;
                maxCount = count;
            }
        }

        String str = maxCountName + "," + maxCount + "\n";
        outputStream.write(str.getBytes());

        lineReader.close();
        outputStream.close();
    }

    public static void combineResult(String from_path_1, String from_path_2, String to_path) throws IOException {
        Path path_from_1 = new Path(from_path_1);
        Configuration configuration_1 = new Configuration();
        FileSystem fileSystem_1 = path_from_1.getFileSystem(configuration_1);
        FSDataInputStream inputStream_1 = fileSystem_1.open(path_from_1);
        LineReader lineReader_1 = new LineReader(inputStream_1, configuration_1);

        Text line_1 = new Text();
        lineReader_1.readLine(line_1);
        String[] parts_1 = line_1.toString().split(",");
        String userId = parts_1[0];
        lineReader_1.close();

        Path path_from_2 = new Path(from_path_2);
        Configuration configuration_2 = new Configuration();
        FileSystem fileSystem_2 = path_from_2.getFileSystem(configuration_2);
        FSDataInputStream inputStream_2 = fileSystem_2.open(path_from_2);
        LineReader lineReader_2 = new LineReader(inputStream_2, configuration_2);

        Text line_2 = new Text();
        lineReader_2.readLine(line_2);
        String[] parts_2 = line_2.toString().split(",");
        String artistId = parts_2[0];
        lineReader_2.close();

        Path path_to = new Path(to_path);
        FSDataOutputStream outputStream = fileSystem_1.create(path_to);

        String str = userId + "," + artistId + "\n";
        outputStream.write(str.getBytes());

        outputStream.close();
    }
}
