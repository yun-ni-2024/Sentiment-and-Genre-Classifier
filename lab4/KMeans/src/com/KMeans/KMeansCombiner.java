package com.KMeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombiner extends Reducer<IntWritable, PointFreqPair, IntWritable, PointFreqPair> {
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
            for (int i = 0; i < newCentroid.length; i++) {
                newCentroid[i] /= totalFreq;
            }

            context.write(key, new PointFreqPair(newCentroid, totalFreq));
        }
    }
}
