package com.WordFrequency;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WordFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
    Map<String, String> genreMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = cacheFiles[0];
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));

            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] parts = line.split(",");
                String trackId = parts[0];
                String genre = parts[1];
                genreMap.put(trackId, genre);
            }

            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",", 2);
        String trackId = parts[0];
        String freq = parts[1].replace("[", "").replace("]", "");

        String genre = genreMap.get(trackId);
        String[] freqParts = freq.split(",");
        for (String pairStr : freqParts) {
            pairStr = pairStr.replace("(", "").replace(")", "");
            String[] pair = pairStr.split(":");
            String word = pair[0];
            int count = Integer.parseInt(pair[1]);

            context.write(new Text(genre + "," + word), new IntWritable(count));
        }
    }
}
