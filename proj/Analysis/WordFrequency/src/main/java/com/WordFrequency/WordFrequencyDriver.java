package com.WordFrequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class WordFrequencyDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

        filterGenresLyrics(inputPath);

        Job job = Job.getInstance(conf, "Analysis Word Frequency");

        job.addCacheFile(new Path(inputPath + "/genres_task23.txt").toUri());

        job.setJarByClass(WordFrequencyDriver.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setReducerClass(WordFrequencyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inputPath + "/lyrics_task23.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/tmp_Analysis_Word_Frequency"));

        job.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        Path folderPath = new Path(outputPath + "/tmp_Word_Frequency");
        FileStatus[] fileStatuses = fs.listStatus(folderPath);
        for (FileStatus status : fileStatuses) {
            Path tmpFilePath = status.getPath();
            String tmpFileName = tmpFilePath.getName();
            String[] parts = tmpFileName.split("-");
            String genre = parts[0];
            if (parts.length == 3 && !genre.equals("part")) {
                copyFile(tmpFilePath.toString(), outputPath + "/task24/" + genre + ".txt");
            }
        }

        deletePath(outputPath + "/tmp_Word_Frequency", true);
    }

    public static void filterGenresLyrics(String pathStr) throws IOException {
        Text line = new Text();

        Path path_genres = new Path(pathStr + "/genres.txt");
        Configuration configuration_genres = new Configuration();
        FileSystem fileSystem_genres = path_genres.getFileSystem(configuration_genres);
        FSDataInputStream inputStream_genres = fileSystem_genres.open(path_genres);
        LineReader lineReader_genres = new LineReader(inputStream_genres, configuration_genres);
        Set<String> genresTrackId = new HashSet<>();
        while(lineReader_genres.readLine(line) > 0) {
            String[] parts = line.toString().split(",");
            String trackId = parts[0];
            genresTrackId.add(trackId);
        }
        lineReader_genres.close();

        Path path_lyrics = new Path(pathStr + "/lyrics.txt");
        Configuration configuration_lyrics = new Configuration();
        FileSystem fileSystem_lyrics = path_genres.getFileSystem(configuration_lyrics);
        FSDataInputStream inputStream_lyrics = fileSystem_genres.open(path_lyrics);
        LineReader lineReader_lyrics = new LineReader(inputStream_lyrics, configuration_lyrics);
        Set<String> lyricsTrackId = new HashSet<>();
        while(lineReader_lyrics.readLine(line) > 0) {
            String[] parts = line.toString().split(",");
            String trackId = parts[0];
            lyricsTrackId.add(trackId);
        }
        lineReader_lyrics.close();

        Set<String> commonTrackId = new HashSet<>(genresTrackId);
        commonTrackId.retainAll(lyricsTrackId);

        Path path_filtered_genres = new Path(pathStr + "/genres_task23.txt");
        FSDataOutputStream outputStream_genres = fileSystem_genres.create(path_filtered_genres);
        inputStream_genres = fileSystem_genres.open(path_genres);
        lineReader_genres = new LineReader(inputStream_genres, configuration_genres);
        while(lineReader_genres.readLine(line) > 0) {
            String[] parts = line.toString().split(",");
            String trackId = parts[0];
            if (commonTrackId.contains(trackId)) {
                String str = line.toString() + "\n";
                outputStream_genres.write(str.getBytes());
            }
        }
        outputStream_genres.close();

        Path path_filtered_lyrics = new Path(pathStr + "/lyrics_task23.txt");
        FSDataOutputStream outputStream_lyrics = fileSystem_genres.create(path_filtered_lyrics);
        inputStream_lyrics = fileSystem_genres.open(path_lyrics);
        lineReader_lyrics = new LineReader(inputStream_lyrics, configuration_lyrics);
        while(lineReader_lyrics.readLine(line) > 0) {
            String[] parts = line.toString().split(",");
            String trackId = parts[0];
            if (commonTrackId.contains(trackId)) {
                String str = line.toString() + "\n";
                outputStream_lyrics.write(str.getBytes());
            }
        }
        outputStream_lyrics.close();
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
