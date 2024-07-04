package com.Classification;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ClassificationPartitioner extends Partitioner<IndexPair, Text> {
    @Override
    public int getPartition(IndexPair key, Text value, int numPartitions) {
        // Use the centerId for partitioning
        int term = key.getCenterId();
        return term;
    }
}
