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

        Job job = Job.getInstance(conf, "Word Frequency");

        job.addCacheFile(new Path(inputPath + "/filtered_genres.txt").toUri());

        job.setJarByClass(WordFrequencyDriver.class);
        job.setMapperClass(WordFrequencyMapper.class);
        job.setReducerClass(WordFrequencyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inputPath + "/filtered_lyrics.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/tmp_Word_Frequency"));

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
