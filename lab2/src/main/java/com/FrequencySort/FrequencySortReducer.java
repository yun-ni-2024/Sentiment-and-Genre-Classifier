package com.FrequencySort;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FrequencySortReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<FloatStringPair> pairList = new ArrayList<>();

        // Populate the pairList with FloatStringPair instances
        for (Text value : values) {
            // Get a <word, frequency> pair
            String[] parts = value.toString().split("[\\s,]+");
            String word = parts[0];
            float freq = Float.parseFloat(parts[1]);

            FloatStringPair pair = new FloatStringPair(freq, word);
            pairList.add(pair);
        }

        // Sort the pairList based on the float values
        Collections.sort(pairList);

        for (int i = pairList.size() - 1; i >= pairList.size() - 50; i--) {
            FloatStringPair pair = pairList.get(i);
            float freq = pair.getFloatValue();
            String word = pair.getStringValue();
            context.write(null, new Text(word));
        }
    }
}
