package com.Preference;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ArtistSongCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    Map<String, String> artistIdMap = new HashMap<>();

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
                String songId = parts[0];
                String artistId = parts[4];
                artistIdMap.put(songId, artistId);
            }

            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        String songId = parts[1];
        int count = Integer.parseInt(parts[2]);

        if (artistIdMap.containsKey(songId)) {
            String artistId = artistIdMap.get(songId);
            context.write(new Text(artistId), new IntWritable(count));
        }
    }
}
