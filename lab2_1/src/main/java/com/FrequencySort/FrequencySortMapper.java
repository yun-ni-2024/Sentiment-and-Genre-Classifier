package com.FrequencySort;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FrequencySortMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Get a <word, frequency> pair
        String[] parts = value.toString().split("[\\s,]+");

        // Output a <word, frequency> pair
        String word = parts[0];
        String freqStr = parts[1];
        String combinedValue = word + "," + freqStr;
        context.write(new Text("default"), new Text(combinedValue));
    }
}
