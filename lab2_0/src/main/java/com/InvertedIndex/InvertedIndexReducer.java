package com.InvertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int documentCount = 0;
        int totalFrequency = 0;
        HashMap<Text, Integer> frequencies = new HashMap<>();

        for (Text fileName: values) {
            if (frequencies.containsKey(fileName)) {
                int frequency = frequencies.get(fileName);
                frequencies.put(fileName, frequency + 1);
            } else {
                frequencies.put(fileName, 1);
                documentCount++;
            }
            totalFrequency++;
        }

        double averageFrequency = (double) totalFrequency / documentCount;
        String outputValue = String.format("%.2f", averageFrequency) + ",";

        Iterator<Map.Entry<Text, Integer>> itr = frequencies.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<Text, Integer> entry = itr.next();
            Text fileName = entry.getKey();
            int frequency = entry.getValue();
            outputValue += fileName + ":" + String.format("%d", frequency) + ";";
        }

        context.write(key, new Text(outputValue));
    }
}
