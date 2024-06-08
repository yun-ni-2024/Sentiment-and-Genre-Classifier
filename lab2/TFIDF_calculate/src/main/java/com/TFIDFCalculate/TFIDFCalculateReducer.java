package com.TFIDFCalculate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFCalculateReducer extends Reducer<Text, Text, StringPair, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String word = key.toString();
        HashMap<String, Integer> frequencies = new HashMap<>();

        // Number of files containing the current word
        int fileNum = 0;

        // Calculate the number of the files containing current word
        for (Text item : values) {
            String fileName = item.toString();
            if (frequencies.containsKey(fileName)) {
                int frequency = frequencies.get(fileName);
                frequencies.put(fileName, frequency + 1);
            } else {
                frequencies.put(fileName, 1);
                fileNum++;
            }
        }

        // For each file containing this word, calculate the TF-IDF value
        Iterator<Map.Entry<String, Integer>> itr = frequencies.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, Integer> entry = itr.next();
            String fileName = entry.getKey();
            int TF = entry.getValue();
            double IDF = Math.log((double)40 / (fileNum + 1)) / Math.log(2);
            double TFIDF = (double)TF * IDF;
            StringPair keyPair = new StringPair(fileName, word);
            context.write(keyPair, new Text(String.format("%.2f", TFIDF)));
        }
    }
}
