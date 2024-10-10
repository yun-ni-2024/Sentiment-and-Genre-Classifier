package com.PrepUsers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PrepUsersMapper extends Mapper<Object, Text, Text, Text> {
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (line.charAt(0) != '#') {
            String[] parts = line.split("\t");

            String userId = parts[0];
            String songId = parts[1];
            String playCount = parts[2];

            context.write(new Text(userId), new Text(userId + "," + songId + "," + playCount));
        }
    }
}
