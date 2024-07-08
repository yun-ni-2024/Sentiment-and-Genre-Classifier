package GenreCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GenreCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        String genre = parts[1];

        context.write(new Text(genre), new IntWritable(1));
    }
}
