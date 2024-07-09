package com.Preprocess;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FilterSongsMapper extends Mapper<Object, Text, Text, Text> {
    Set<String> songIdSet = new HashSet<>();
    Set<String> trackIdSet = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            // lyrics.txt
            Path path_lyrics = cacheFiles[0];
            BufferedReader reader_lyrics = new BufferedReader(new FileReader(path_lyrics.toString()));
            String line_lyrics;
            while ((line_lyrics = reader_lyrics.readLine()) != null)
            {
                String[] parts = line_lyrics.split(",");
                String trackId = parts[0];
                if (!trackIdSet.contains(trackId)) {
                    trackIdSet.add(trackId);
                }
            }
            reader_lyrics.close();

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

            // users.txt
            Path path_users = cacheFiles[2];
            BufferedReader reader_users = new BufferedReader(new FileReader(path_users.toString()));
            String line_users;
            while ((line_users = reader_users.readLine()) != null)
            {
                String[] parts = line_users.split(",");
                String songId = parts[1];
                if (!songIdSet.contains(songId)) {
                    songIdSet.add(songId);
                }
            }
            reader_users.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        String songId = parts[0];
        String trackId = parts[1];
        if (songIdSet.contains(songId) || trackIdSet.contains(trackId)) {
            context.write(new Text(songId), value);
        }
    }
}
