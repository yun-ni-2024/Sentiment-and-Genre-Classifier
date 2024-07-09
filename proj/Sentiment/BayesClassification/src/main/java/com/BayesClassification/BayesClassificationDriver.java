package com.BayesClassification;

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

public class BayesClassificationDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String freqPath = args[0];
        String testPath = args[1];
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "Bayes Classification");

        job.addCacheFile(new Path(freqPath).toUri());

        job.setJarByClass(BayesClassificationDriver.class);
        job.setMapperClass(BayesClassificationMapper.class);
        job.setReducerClass(BayesClassificationReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(testPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/tmp_Bayes_Classification"));

        job.waitForCompletion(true);

        copyFile(outputPath + "/tmp_Bayes_Classification/part-r-00000", outputPath + "/task3.txt");
        deletePath(outputPath + "/tmp_Bayes_Classification", true);
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
