package ArtistGenreBayesTraining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

public class CommaSeparatedOutputFormat extends FileOutputFormat<Text, IntWritable> {
    @Override
    public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = getDefaultWorkFile(context, "");
        FileSystem fs = file.getFileSystem(conf);

        // Use a custom RecordWriter to specify a different separator
        return new MyRecordWriter(fs.create(file, false));
    }

    private static class MyRecordWriter extends RecordWriter<Text, IntWritable> {
        private DataOutputStream out;

        public MyRecordWriter(DataOutputStream out) {
            this.out = out;
        }

        @Override
        public void write(Text key, IntWritable value) throws IOException {
            // Customize the output format here
            out.writeBytes(key.toString() + "," + value.toString() + "\n");
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }
}
