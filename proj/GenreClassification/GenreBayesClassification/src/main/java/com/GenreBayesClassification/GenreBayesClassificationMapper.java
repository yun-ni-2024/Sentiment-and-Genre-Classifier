package com.GenreBayesClassification;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GenreBayesClassificationMapper extends Mapper<Object, Text, Text, Text> {
    Map<String, Double> labelFreq = new HashMap<>();
    Map<String, Double> attrFreq = new HashMap<>();
    int totalTracks = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = cacheFiles[0];
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] parts = line.split("<SEP>");
                if (parts.length == 2) {
                    String name = parts[0];
                    int freq = Integer.parseInt(parts[1]);
                    labelFreq.put(name, (double) freq);
                } else {
                    String name = parts[0] + "<SEP>" + parts[1] + "<SEP>" + parts[2];
                    int freq = Integer.parseInt(parts[3]);
                    attrFreq.put(name, (double) freq);
                    totalTracks++;
                }
            }
            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.equals("")) {
            return;
        }

        String[] parts = line.split("<SEP>", -1);
        String trackId = parts[0];

        String artistName = parts[2];
        String[] artistWords = artistName.split(" ");
        String songName = parts[3];
        String[] songWords = songName.split(" ");
        double maxF = -10000000.0;
        String maxFLabel = "none";

        for (Map.Entry<String, Double> entry : labelFreq.entrySet()) {
            String label = entry.getKey();
            double FXYi = 0.0;

            for (String word : artistWords) {
                String attrItem = label + "<SEP>a<SEP>" + word;
                if (attrFreq.containsKey(attrItem)) {
                    FXYi += Math.log(attrFreq.get(attrItem)) - Math.log(labelFreq.get(label));
                } else {
                    FXYi += Math.log(1 / (labelFreq.get(label) + totalTracks));
                }
            }

            for (String word : songWords) {
                String attrItem = label + "<SEP>s<SEP>" + word;
                if (attrFreq.containsKey(attrItem)) {
                    FXYi += Math.log(attrFreq.get(attrItem)) - Math.log(labelFreq.get(label));
                } else {
                    FXYi += Math.log(1 / (labelFreq.get(label) + totalTracks));
                }
            }

            if (FXYi > maxF) {
                maxF = FXYi;
                maxFLabel = label;
            }
        }

        context.write(new Text(trackId), new Text(maxFLabel));
    }
}