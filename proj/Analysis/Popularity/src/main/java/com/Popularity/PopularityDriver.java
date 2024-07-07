package com.Popularity;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PopularityDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance(conf, "Analyze song popularity");

        job.addCacheFile(new Path(inputPath + "/songs.txt").toUri());

        job.setJarByClass(PopularityDriver.class);
        job.setMapperClass(PopularityMapper.class);
        job.setReducerClass(PopularityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(CommaSeparatedOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(inputPath + "/filtered_users.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/tmp_Analyze_Song_Popularity"));

        job.waitForCompletion(true);

        copyFileAndSort(outputPath + "/tmp_Analyze_Song_Popularity/part-r-00000", outputPath + "/task21.txt");
        deletePath(outputPath + "/tmp_Analyze_Song_Popularity", true);
    }

    public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    public static void copyFileAndSort(String from_path, String to_path) throws IOException {
        Path path_from = new Path(from_path);
        Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);

        Text line = new Text();
        List<StringIntPair> list = new ArrayList<>();
        while(lineReader.readLine(line) > 0) {
            String[] parts = line.toString().split(",");
            String title = parts[0];
            int count = Integer.parseInt(parts[1]);
            list.add(new StringIntPair(title, count));
        }

        Collections.sort(list);
        for (int i = 0; i < 10; ++i) {
            String str = list.get(i).getTitle() + "," + list.get(i).getCount() + "\n";
            outputStream.write(str.getBytes());
        }

        lineReader.close();
        outputStream.close();
    }

    public static class StringIntPair implements Comparable<StringIntPair> {
        private String title;
        private int count;

        public StringIntPair(String title, int count) {
            this.title = title;
            this.count = count;
        }

        public String getTitle() {
            return title;
        }

        public int getCount() {
            return count;
        }

        @Override
        public int compareTo(StringIntPair other) {
            return Integer.compare(other.getCount(), this.count); // 降序排列
        }
    }
}
