package com.Classification;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ClassificationReducer extends Reducer<IndexPair, Text, IntWritable, Text> {
    private MultipleOutputs<IntWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(IndexPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            int centerId = key.getCenterId();
            int pointId = key.getPointId();
            String pointStr = value.toString().trim();
            multipleOutputs.write(new IntWritable(pointId), new Text(pointStr), Integer.toString(centerId));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
