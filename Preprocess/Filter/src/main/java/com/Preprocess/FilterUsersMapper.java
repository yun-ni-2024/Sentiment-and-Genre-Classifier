package com.Preprocess;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FilterUsersMapper extends Mapper<Object, Text, Text, Text> {
    Set<String> songIdSet = new HashSet<>();

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
                songIdSet.add(songId);
            }

            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        String userId = parts[0];
        String songId = parts[1];
        if (songIdSet.contains(songId)) {
            context.write(new Text(userId), value);
        }
    }
}