package com.BayesClassification;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BayesClassificationMapper extends Mapper<Object, Text, Text, IntWritable> {
    Map<String, Integer> labelFreq = new HashMap<>();
    Map<String, Integer> attrFreq = new HashMap<>();

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
                String name = parts[0];
                int freq = Integer.parseInt(parts[1]);
                String[] nameParts = name.split(":");

                if (nameParts.length == 1) {
                    labelFreq.put(name, freq);
                } else {
                    attrFreq.put(name, freq);
                }
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
        int[] attrValues = new int[6000];
        Arrays.fill(attrValues, 0);
        for (int i = 2; i < parts.length; ++i) {
            String[] pair = parts[i].split(":");
            int attrIndex = Integer.parseInt(pair[0]);
            int attrValue = Integer.parseInt(pair[1]);
            attrValues[attrIndex] = attrValue;
        }

        int maxF = 0;
        String maxFLabel = "none";
        for (Map.Entry<String, Integer> entry : labelFreq.entrySet()) {
            String label = entry.getKey();
            int FXYi = 1;
            int FYi = entry.getValue();
            for (int i = 1; i <= 5000; ++i) {
                int attrIndex = i;
                int attrValue = attrValues[attrIndex];
                int FxYij = attrFreq.get(label + ":" + attrIndex + ":" + attrValue);
                FXYi *= FxYij;
            }
            if (FXYi * FYi > maxF) {
                maxF = FXYi * FYi;
                maxFLabel = label;
            }
        }

        context.write(new Text(trackId), new Text(maxFLabel));
    }
}
