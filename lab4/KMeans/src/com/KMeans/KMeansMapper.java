package com.KMeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, PointFreqPair> {
    private Map<Integer, double[]> centers = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path [] cacheFiles = context.getLocalCacheFiles();
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
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        double[] point = new double[data.length];
        for (int i = 0; i < data.length; i++) {
            point[i] = Double.parseDouble(data[i].trim());
        }

        int index = -1;
        double minDis = Double.MAX_VALUE;

        for (Map.Entry<Integer, double[]> entry : centers.entrySet()) {
            int centroidId = entry.getKey();
            double[] centroid = entry.getValue();

            double dis = 0.0;
            for (int i = 0; i < point.length; i++) {
                dis += Math.pow(point[i] - centroid[i], 2);
            }

            if (dis < minDis) {
                minDis = dis;
                index = centroidId;
            }
        }

        context.write(new IntWritable(index), new PointFreqPair(point, 1));
    }
}
