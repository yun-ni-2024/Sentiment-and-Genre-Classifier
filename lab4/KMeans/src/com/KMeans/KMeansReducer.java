package com.KMeans;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KMeansReducer extends Reducer<IntWritable, PointFreqPair, NullWritable, Text> {
    private Map<Integer, double[]> centers = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = cacheFiles[0];
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] parts = line.split(":");
                int centroidId = Integer.parseInt(parts[0].trim());
                String[] centroidStr = parts[1].split(",");
                double[] centroid = new double[centroidStr.length];
                for (int i = 0; i < centroidStr.length; i++) {
                    centroid[i] = Double.parseDouble(centroidStr[i].trim());
                }
                centers.put(centroidId, centroid);
            }
            reader.close();
        }
    }

    @Override
    public void reduce(IntWritable key, Iterable<PointFreqPair> values, Context context) throws IOException, InterruptedException {
        int centroidId = key.get();
        double[] newCentroid = null;
        int totalFreq = 0;

        for (PointFreqPair value : values) {
            double[] point = value.getPoint();
            int freq = value.getFreq();

            if (newCentroid == null) {
                newCentroid = new double[point.length];
                for (int j = 0; j < newCentroid.length; j++) {
                    newCentroid[j] = 0.0;
                }
            }
            for (int i = 0; i < newCentroid.length; i++) {
                newCentroid[i] += point[i] * freq;
            }
            totalFreq += freq;
        }

        if (newCentroid != null && totalFreq > 0) {
            StringBuilder newCentroidStr = new StringBuilder();

            newCentroidStr.append(key.get()).append(": ");

            for (int i = 0; i < newCentroid.length; i++) {
                newCentroid[i] /= totalFreq;
                if (i == 0) {
                    newCentroidStr.append(newCentroid[i]);
                } else {
                    newCentroidStr.append(", ").append(newCentroid[i]);
                }
            }

            if (Arrays.equals(newCentroid, centers.get(centroidId))) {
                newCentroidStr.append(": ").append("0");
            } else {
                newCentroidStr.append(": ").append("1");
            }

            context.write(null, new Text(newCentroidStr.toString()));
        }
    }
}
