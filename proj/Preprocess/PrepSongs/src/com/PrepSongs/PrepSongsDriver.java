package com.PrepSongs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PrepSongsDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance(conf, "Preprocess songs");

        job.setJarByClass(PrepSongsDriver.class);
        job.setMapperClass(PrepSongsMapper.class);
        job.setReducerClass(PrepSongsReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
//        FileInputFormat.setInputDirRecursive(job, true);  // Enable recursive input path traversal
        TextInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

//        job.waitForCompletion(true);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PrepSongsDriver(), args);
        System.exit(res);
    }
}
