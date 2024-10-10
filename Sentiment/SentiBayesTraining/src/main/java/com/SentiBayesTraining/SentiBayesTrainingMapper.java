package com.SentiBayesTraining;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SentiBayesTrainingMapper extends Mapper<Object, Text, Text, IntWritable> {
    Map<String, String> sentimentMap = new HashMap<>();

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
                String sentiment = parts[1];
                sentimentMap.put(trackId, sentiment);
            }

            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("#") || line.startsWith("%")) {
            return;
        }

        String[] parts = line.split(",");
        String trackId = parts[0];

        if (sentimentMap.containsKey(trackId)) {
            String label = sentimentMap.get(trackId);
            context.write(new Text(label), new IntWritable(1));

            for (int i = 2; i < parts.length; ++i) {
                String[] pair = parts[i].split(":");
                int attrIndex = Integer.parseInt(pair[0]);
                int attrValue = Integer.parseInt(pair[1]);
                context.write(new Text(label + ":" + attrIndex + ":" + attrValue), new IntWritable(1));
            }
        }
    }
}
