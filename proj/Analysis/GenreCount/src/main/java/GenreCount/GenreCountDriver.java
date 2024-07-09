package GenreCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class GenreCountDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

        Job job = Job.getInstance(conf, "Genre Count");
        job.setJarByClass(GenreCountDriver.class);
        job.setMapperClass(GenreCountMapper.class);
        job.setReducerClass(GenreCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(inputPath + "/filtered_genres.txt"));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "/tmp_Genre_Count"));
        job.waitForCompletion(true);

        getHighest(outputPath + "/tmp_Genre_Count/part-r-00000", outputPath + "/genre_highest.txt");
        deletePath(outputPath + "/tmp_Genre_Count", true);
    }

    public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }

    public static void getHighest(String from_path, String to_path) throws IOException {
        Path path_from = new Path(from_path);
        Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);

        Text line = new Text();
        String maxCountName = "none";
        int maxCount = 0;

        while(lineReader.readLine(line) > 0) {
            String[] parts = line.toString().split("\t");
            String name = parts[0];
            int count = Integer.parseInt(parts[1]);

            if (count > maxCount) {
                maxCountName = name;
                maxCount = count;
            }
        }

        String str = maxCountName + "," + maxCount + "\n";
        outputStream.write(str.getBytes());

        lineReader.close();
        outputStream.close();
    }
}
