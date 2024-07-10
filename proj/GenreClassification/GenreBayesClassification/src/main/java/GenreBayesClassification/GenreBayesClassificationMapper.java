package GenreBayesClassification;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GenreBayesClassificationMapper extends Mapper<Object, Text, Text, Text> {
    Map<String, Double> artistLabelFreq = new HashMap<>();
    Map<String, Double> artistAttrFreq = new HashMap<>();
    Map<String, Double> songLabelFreq = new HashMap<>();
    Map<String, Double> songAttrFreq = new HashMap<>();
    double alpha = 1;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            // artist
            Path path_0 = cacheFiles[0];
            BufferedReader reader_0 = new BufferedReader(new FileReader(path_0.toString()));
            String line_0;
            while ((line_0 = reader_0.readLine()) != null)
            {
                String[] parts = line_0.split(",");
                String name = parts[0];
                int freq = Integer.parseInt(parts[1]);
                String[] nameParts = name.split(":");

                if (nameParts.length == 1) {
                    artistLabelFreq.put(name, (double) freq + alpha);
                } else {
                    artistAttrFreq.put(name, (double) freq);
                }
            }
            reader_0.close();

            // song
            Path path_1 = cacheFiles[1];
            BufferedReader reader_1 = new BufferedReader(new FileReader(path_1.toString()));
            String line_1;
            while ((line_1 = reader_1.readLine()) != null)
            {
                String[] parts = line_1.split(",");
                String name = parts[0];
                int freq = Integer.parseInt(parts[1]);
                String[] nameParts = name.split(":");

                if (nameParts.length == 1) {
                    songLabelFreq.put(name, (double) freq + alpha);
                } else {
                    songAttrFreq.put(name, (double) freq);
                }
            }
            reader_1.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.equals("")) {
            return;
        }

        String[] parts = line.split("<SEP>", -1);
        String trackId = parts[0];

        Map<String, Double> labelF = new HashMap<>();

        String artistName = parts[2];
        String[] artistWords = artistName.split("\\W+");
        for (String word : artistWords) {
            int[] attrValues = new int[60];
            for (int i = 0; i < word.length(); ++i) {
                int attrIndex = i;
                int attrValue = word.charAt(i);
                attrValues[attrIndex] = attrValue;
            }
            for (int i = word.length(); i < 50; ++i) {
                int attrIndex = i;
                int attrValue = 0;
                attrValues[attrIndex] = attrValue;
            }

            for (Map.Entry<String, Double> entry : artistLabelFreq.entrySet()) {
                String label = entry.getKey();
                Double FXYi = 0.0;
                Double FYi = Math.log(entry.getValue());
                for (int i = 0; i < 50; ++i) {
                    int attrIndex = i;
                    int attrValue = attrValues[attrIndex];
                    String attrItem = label + ":" + attrIndex + ":" + attrValue;
                    if (artistAttrFreq.containsKey(attrItem)) {
                        FXYi += Math.log(artistAttrFreq.get(attrItem));
                    }
                    FXYi -= Math.log(entry.getValue());
                }
                if (labelF.containsKey(label)) {
                    labelF.put(label, labelF.get(label) + FXYi);
                } else {
                    labelF.put(label, FXYi);
                }
            }
        }

        String songName = parts[3];
        String[] songWords = songName.split("\\W+");
        for (String word : songWords) {
            int[] attrValues = new int[120];
            for (int i = 0; i < word.length(); ++i) {
                int attrIndex = i;
                int attrValue = word.charAt(i);
                attrValues[attrIndex] = attrValue;
            }
            for (int i = word.length(); i < 100; ++i) {
                int attrIndex = i;
                int attrValue = 0;
                attrValues[attrIndex] = attrValue;
            }

            for (Map.Entry<String, Double> entry : songLabelFreq.entrySet()) {
                String label = entry.getKey();
                Double FXYi = 0.0;
                Double FYi = Math.log(entry.getValue());
                for (int i = 0; i < 100; ++i) {
                    int attrIndex = i;
                    int attrValue = attrValues[attrIndex];
                    String attrItem = label + ":" + attrIndex + ":" + attrValue;
                    if (songAttrFreq.containsKey(attrItem)) {
                        FXYi += Math.log(songAttrFreq.get(attrItem));
                    }
                    FXYi -= Math.log(entry.getValue());
                }
                if (labelF.containsKey(label)) {
                    labelF.put(label, labelF.get(label) + FXYi);
                } else {
                    labelF.put(label, FXYi);
                }
            }
        }

        Double maxF = -1000000.0;
        String maxFLabel = "none";
        for (Map.Entry<String, Double> entry : labelF.entrySet()) {
            String label = entry.getKey();
            Double F = entry.getValue();
            if (F > maxF) {
                maxF = F;
                maxFLabel = label;
            }
        }

        context.write(new Text(trackId), new Text(maxFLabel));
    }
}
