package com.SentiBayesTraining;

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

public class SentiBayesTrainingDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String labelPath = args[0];
        String trainPath = args[1];
        String outputPath = args[2];

        Job job = Job.getInstance(conf, "Senti Bayes Training");

        job.addCacheFile(new Path(labelPath).toUri());

        job.setJarByClass(SentiBayesTrainingDriver.class);
        job.setMapperClass(SentiBayesTrainingMapper.class);
        job.setReducerClass(SentiBayesTrainingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(trainPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/tmp_Senti_Bayes_Training"));

        job.waitForCompletion(true);

        copyFile(outputPath + "/tmp_Senti_Bayes_Training/part-r-00000", outputPath + "/senti_bayes_frequency.txt");
        deletePath(outputPath + "/tmp_Senti_Bayes_Training", true);
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
