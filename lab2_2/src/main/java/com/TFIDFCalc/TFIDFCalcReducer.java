package com.TFIDFCalc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFCalcReducer extends Reducer<Text, Text, StringPair, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String word = key.toString();
        HashMap<String, Integer> frequencies = new HashMap<>();
        List<Text> fileNames = new ArrayList<>();

        int fileNum = 0;
        for (Text fileName : values) {
            // Store the file name in the list
            fileNames.add(new Text(fileName));

            fileNum++;
        }

        for (Text item : fileNames) {
            String fileName = item.toString();
            if (frequencies.containsKey(fileName)) {
                int frequency = frequencies.get(fileName);
                frequencies.put(fileName, frequency + 1);
            } else {
                frequencies.put(fileName, 1);
            }
        }

//        for (StringIntPair value : values) {
////            String fileName = value.getFirst();
////            int TF = value.getSecond();
////            double IDF = Math.log((double)40 / (fileNum + 1)) / Math.log(2);
////            context.write(new StringPair(fileName, word), new DoubleWritable(TF * IDF));
////            context.write(new Text(word), new StringIntPair(fileName, TF));
//            context.write(key, value);
//        }

        Iterator<Map.Entry<String, Integer>> itr = frequencies.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<String, Integer> entry = itr.next();
            String fileName = entry.getKey();
            int TF = entry.getValue();
            double IDF = Math.log((double)40 / (fileNum + 1)) / Math.log(2);
            double TFIDF = (double)TF * IDF;
            StringPair keyPair = new StringPair(fileName, word);
//            context.write(new Text(fileName + " " + word), new Text(String.format("%.2f", TFIDF)));
            context.write(keyPair, new Text(String.format("%.2f", TFIDF)));
//            context.write(keyPair, new DoubleWritable(TF * IDF));
        }
//        context.write(key, new Text(String.valueOf(num)));


//        int fileNum = 0;
//        for (StringIntPair value : values) {
//            fileNum++;
//        }
//
//        for (StringIntPair value : values) {
//            String fileName = value.getFirst();
//            int TF = value.getSecond();
//            double IDF = Math.log((double)40 / (fileNum + 1)) / Math.log(2);
//            StringPair keyPair = new StringPair(fileName, word);
//            context.write(keyPair, new DoubleWritable(TF * IDF));
//        }
    }
}
