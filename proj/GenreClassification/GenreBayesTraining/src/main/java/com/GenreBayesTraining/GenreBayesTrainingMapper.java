package com.GenreBayesTraining;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GenreBayesTrainingMapper extends Mapper<Object, Text, Text, IntWritable> {
    Map<String, String> genreMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = cacheFiles[0];
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));

            String line;
            while ((line = reader.readLine()) != null)
            {
                String[] parts = line.split(",");
                String trackId = parts[0];
                String genre = parts[1];
                genreMap.put(trackId, genre);
            }

            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.equals("")) {
            return;
        }

        String[] parts = line.split("<SEP>", -1);
        String trackId = parts[0];

        if (genreMap.containsKey(trackId)) {
            String label = genreMap.get(trackId);

            String artistName = parts[2];
            String[] wordsArtist = artistName.split(" ");
            for (String word : wordsArtist) {
                context.write(new Text(label + "<SEP>a<SEP>" + word), new IntWritable(1));
                context.write(new Text(label), new IntWritable(1));
            }

            String songName = parts[3];
            String[] wordsSong = songName.split(" ");
            for (String word : wordsSong) {
                context.write(new Text(label + "<SEP>s<SEP>" + word), new IntWritable(1));
                context.write(new Text(label), new IntWritable(1));
            }
        }
    }
}
