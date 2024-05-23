package com.TFIDFCalc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;

public class TFIDFCalcMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("Processing input: " + value.toString());
        // Get the name of current file
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        Path filePath = fileSplit.getPath();
        String fileName = filePath.getName();

        // Iterate over the text
        StringTokenizer itr = new StringTokenizer(value.toString());
//        HashMap<String, Integer> frequencies = new HashMap<>();
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            String cleanedWord = word.replaceAll("\\p{Punct}", "").toLowerCase();
            if (!cleanedWord.isEmpty()) {
                context.write(new Text(cleanedWord), new Text(fileName));
            }
//            if (!cleanedWord.isEmpty()) {
//                frequencies.put(word, frequencies.getOrDefault(word, 0) + 1);
////                if (frequencies.containsKey(cleanedWord)) {
////                    int frequency = frequencies.get(cleanedWord);
////                    frequencies.put(cleanedWord, frequency + 1);
////                } else {
////                    frequencies.put(cleanedWord, 1);
////                }
//            }
        }

//        Iterator<Map.Entry<String, Integer>> mapItr = frequencies.entrySet().iterator();
//        while (mapItr.hasNext()) {
//            Map.Entry<String, Integer> entry = mapItr.next();
//            String word = entry.getKey();
//            int freq = entry.getValue();
//            context.write(new Text(word + " " + Integer.toString(word.length())), new StringIntPair(fileName, freq));
//        }
    }
}
