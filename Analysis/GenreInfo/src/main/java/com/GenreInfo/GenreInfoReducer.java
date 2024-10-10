package com.GenreInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GenreInfoReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double totEnergy = 0.0;
        Double totTempo = 0.0;
        Double totLoudness = 0.0;
        Double totDuration = 0.0;
        Double totDanceability = 0.0;
        int count = 0;

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            Double energy = Double.parseDouble(parts[0]);
            Double tempo = Double.parseDouble(parts[1]);
            Double loudness = Double.parseDouble(parts[2]);
            Double duration = Double.parseDouble(parts[3]);
            Double danceability = Double.parseDouble(parts[4]);

            totEnergy += energy;
            totTempo += tempo;
            totLoudness += loudness;
            totDuration += duration;
            totDanceability += danceability;
            count++;
        }

        totEnergy /= count;
        totTempo /= count;
        totLoudness /= count;
        totDuration /= count;
        totDanceability /= count;

        String feature = totEnergy + "," + totTempo + "," + totLoudness + "," + totDuration + "," + totDanceability;

        context.write(key, new Text(feature));
    }
}
