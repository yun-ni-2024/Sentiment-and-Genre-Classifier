package com.PrepSongs;

//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.RemoteIterator;
//import org.apache.hadoop.fs.LocatedFileStatus;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//
//import java.io.IOException;
//
//public class FilenameInputFormat extends FileInputFormat<LongWritable, Text> {
//
//    @Override
//    protected boolean isSplitable(JobContext context, Path file) {
//        return false;
//    }
//
//    @Override
//    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//        return new FilenameRecordReader();
//    }
//
//    public static class FilenameRecordReader extends RecordReader<LongWritable, Text> {
//        private LongWritable key = new LongWritable(0);
//        private Text value = new Text();
//        private boolean processed = false;
//        private FileSplit fileSplit;
//
//        @Override
//        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//            this.fileSplit = (FileSplit) split;
//        }
//
//        @Override
//        public boolean nextKeyValue() throws IOException, InterruptedException {
//            if (!processed) {
//                value.set(fileSplit.getPath().toString());
//                processed = true;
//                return true;
//            }
//            return false;
//        }
//
//        @Override
//        public LongWritable getCurrentKey() {
//            return key;
//        }
//
//        @Override
//        public Text getCurrentValue() {
//            return value;
//        }
//
//        @Override
//        public float getProgress() {
//            return processed ? 1.0f : 0.0f;
//        }
//
//        @Override
//        public void close() {
//        }
//    }
//}

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FilenameInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> result = new ArrayList<>();
        Path[] dirs = getInputPaths(job);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        for (Path dir : dirs) {
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(dir, true);
            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                result.add(fileStatus);
            }
        }
        return result;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new FilenameRecordReader();
    }

    public static class FilenameRecordReader extends RecordReader<LongWritable, Text> {
        private LongWritable key = new LongWritable(0);
        private Text value = new Text();
        private boolean processed = false;
        private FileSplit fileSplit;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            this.fileSplit = (FileSplit) split;
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                value.set(fileSplit.getPath().toString());
                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() {
        }
    }
}
