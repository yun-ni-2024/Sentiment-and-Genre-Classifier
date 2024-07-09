package com.BayesClassification;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BayesClassificationMapper extends Mapper<Object, Text, Text, Text> {
    Map<String, Double> labelFreq = new HashMap<>();
    Map<String, Double> attrFreq = new HashMap<>();
    Map<String, Double> offset = new HashMap<>();
    double alpha = 0.1;

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
                    labelFreq.put(name, (double) freq);
                } else {
                    attrFreq.put(name, freq + alpha);
                    String label = nameParts[0];
                    String attrInd = nameParts[1];
                    String key = label + ":" + attrInd;
                    if (offset.containsKey(key)) {
                        offset.put(key, offset.get(key) + alpha);
                    } else {
                        offset.put(key, alpha);
                    }
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

        Double maxF = -1000000.0;
        String maxFLabel = "none";
        for (Map.Entry<String, Double> entry : labelFreq.entrySet()) {
            String label = entry.getKey();
            Double FXYi = 0.0;
            Double FYi = Math.log(entry.getValue());
            for (int i = 1; i <= 5000; ++i) {
                int attrIndex = i;
                int attrValue = attrValues[attrIndex];
                String attrItem = label + ":" + attrIndex + ":" + attrValue;
                if (attrFreq.containsKey(attrItem)) {
                    FXYi += Math.log(attrFreq.get(attrItem));
                    FXYi -= Math.log(entry.getValue() + offset.get(label + ":" + i) + alpha);
                }
            }
            if (FXYi + FYi > maxF) {
                maxF = FXYi + FYi;
                maxFLabel = label;
            }
        }

        context.write(new Text(trackId), new Text(maxFLabel));
    }
}
