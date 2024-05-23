//package com.TFIDFCalc;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
//
//public class TFIDFCalcCombiner extends Reducer<Text, StringIntPair, Text, StringIntPair> {
//    @Override
//    public void reduce(Text key, Iterable<StringIntPair> values, Context context) throws IOException, InterruptedException {
//        Text word = key;
//        HashMap<String, Integer> frequencies = new HashMap<>();
//
//        for (StringIntPair value : values) {
//            String fileName = value.getFirst();
//            if (frequencies.containsKey(fileName)) {
//                int frequency = frequencies.get(fileName);
//                frequencies.put(fileName, frequency + 1);
//            } else {
//                frequencies.put(fileName, 1);
//            }
//        }
//
//        Iterator<Map.Entry<String, Integer>> itr = frequencies.entrySet().iterator();
//        while (itr.hasNext()) {
//            Map.Entry<String, Integer> entry = itr.next();
//            String fileName = entry.getKey();
//            int frequency = entry.getValue();
//            context.write(word, new StringIntPair(fileName, frequency));
//        }
//    }
//}
