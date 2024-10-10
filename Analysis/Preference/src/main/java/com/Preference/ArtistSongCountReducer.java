package com.Preference;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ArtistSongCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totCount = 0;
        for (IntWritable value : values) {
            totCount += value.get();
        }

        context.write(key, new IntWritable(totCount));
    }
}
