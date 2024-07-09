package com.Preprocess;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FilterLyricsMapper extends Mapper<Object, Text, Text, Text> {
    Set<String> trackIdSet = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            // songs.txt
            Path path_songs = cacheFiles[0];
            BufferedReader reader_songs = new BufferedReader(new FileReader(path_songs.toString()));
            String line_songs;
            while ((line_songs = reader_songs.readLine()) != null)
            {
                String[] parts = line_songs.split(",");
                String trackId = parts[1];
                if (!trackIdSet.contains(trackId)) {
                trackIdSet.add(trackId);
                }
            }
            reader_songs.close();

            // genres.txt
            Path path_genres = cacheFiles[1];
            BufferedReader reader_genres = new BufferedReader(new FileReader(path_genres.toString()));
            String line_genres;
            while ((line_genres = reader_genres.readLine()) != null)
            {
                String[] parts = line_genres.split(",");
                String trackId = parts[0];
                if (!trackIdSet.contains(trackId)) {
                    trackIdSet.add(trackId);
                }
            }
            reader_genres.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        String trackId = parts[0];
        if (trackIdSet.contains(trackId)) {
            context.write(new Text(trackId), value);
        }
    }
}
