package com.PrepLyrics;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

public class PrepLyricsMapper extends Mapper<Object, Text, Text, Text> {
    String[] wordList = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path [] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = cacheFiles[0];
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));

            String line = reader.readLine();
            String[] parts = line.split("%");
            String wordStr = parts[parts.length - 1];

            String[] words = wordStr.split(",");
            wordList = new String[words.length + 100];
            for (int i = 0; i < words.length; ++i) {
                wordList[i + 1] = words[i];
            }
            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.charAt(0) != '#' && line.charAt(0) != '%') {
            String[] parts = line.split(",");

            String trackId = parts[0];
            String mxmTrackId = parts[1];

            StringBuilder outputValue = new StringBuilder();
            outputValue.append(trackId).append(",[");

            for (int i = 2; i < parts.length; ++i) {
                String[] pair = parts[i].split(":");
                int wordId = Integer.parseInt(pair[0]);
                String count = pair[1];

                outputValue.append("(").append(wordList[wordId]).append(":").append(count).append(")");
                if (i != parts.length - 1) {
                    outputValue.append(",");
                }
            }

            outputValue.append("]");

            context.write(new Text(trackId), new Text(outputValue.toString()));
        }
    }
}
