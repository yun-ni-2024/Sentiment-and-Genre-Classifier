package com.PrepGenres;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PrepGenresMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.charAt(0) != '#') {
            String[] parts = line.split("\t");

            String trackId = parts[0];
            String genre = parts[1];

            context.write(new Text(trackId), new Text(trackId + "," + genre));
        }
    }
}
