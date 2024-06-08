package com.InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Get the name of current file
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        Path filePath = fileSplit.getPath();
        String fileName = filePath.getName();

        // Iterate over the text and output a pair <word, fileName> for each word
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String word = itr.nextToken();
            String cleanedWord = word.replaceAll("\\p{Punct}", "").toLowerCase();
            if (!cleanedWord.isEmpty()) {
                context.write(new Text(cleanedWord), new Text(fileName));
            }
        }
    }
}
