package com.GenreInfo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GenreInfoMapper extends Mapper<Object, Text, Text, Text> {
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
        String[] parts = line.split(",");
        String trackId = parts[1];
        if (genreMap.containsKey(trackId)) {
            String genre = genreMap.get(trackId);

            Double energy = Double.parseDouble(parts[7]);
            Double tempo = Double.parseDouble(parts[8]);
            Double loudness = Double.parseDouble(parts[9]);
            Double duration = Double.parseDouble(parts[10]);
            Double danceability = Double.parseDouble(parts[11]);
            String feature = energy + "," + tempo + "," + loudness + "," + duration + "," + danceability;

            context.write(new Text(genre), new Text(feature));
        }
    }
}
