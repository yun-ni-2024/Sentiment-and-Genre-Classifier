package com.RemoveRedundant;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Set;
import java.util.HashSet;

public class RemoveRedundantLyricsMapper extends Mapper<Object, Text, Text, Text> {
    Set<String> trackIdSet = new HashSet<>();

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
                trackIdSet.add(parts[1]);
            }

            reader.close();
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
