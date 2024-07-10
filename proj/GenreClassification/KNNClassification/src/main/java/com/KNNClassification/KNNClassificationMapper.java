package com.KNNClassification;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class KNNClassificationMapper extends Mapper<Object, Text, Text, Text> {
    List<String> points = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            Path path = cacheFiles[0];
            BufferedReader reader = new BufferedReader(new FileReader(path.toString()));

            String line;
            while ((line = reader.readLine()) != null)
            {
                points.add(line);
            }

            reader.close();
        }
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("<SEP>", -1);
        String trackId = parts[0];
        String currFeature = parts[2] + "<SEP>" + parts[3];

        int k = 3;
        PriorityQueue<Pair> maxHeap = new PriorityQueue<>(k, new Comparator<Pair>() {
            public int compare(Pair a, Pair b) {
                return Integer.compare(b.distance, a.distance); // 最大堆，比较距离的逆序
            }
        });

        for (String point : points) {
            String[] pointParts = point.split("<SEP>", -1);
            String pointFeature = pointParts[0] + "<SEP>" + pointParts[1];
            String label = pointParts[2];

            int dis = calcDistance(currFeature, pointFeature);
            Pair pair = new Pair(dis, label);
            if (maxHeap.size() < k) {
                maxHeap.offer(pair); // 堆中元素数量小于 k，直接加入
            } else {
                // 堆中元素数量已经达到 k，比较并更新堆顶元素
                if (pair.distance < maxHeap.peek().distance) {
                    maxHeap.poll(); // 弹出堆顶元素
                    maxHeap.offer(pair); // 加入当前距离
                }
            }
        }

        Map<String, Integer> count = new HashMap<>();
        while (!maxHeap.isEmpty()) {
            Pair pair = maxHeap.poll();
            String label = pair.getLabel();
            if (count.containsKey(label)) {
                count.put(label, count.get(label) + 1);
            } else {
                count.put(label, 1);
            }
        }
        int maxCount = 0;
        String maxLabel = "none";
        for (Map.Entry<String, Integer> entry : count.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                maxLabel = entry.getKey();
            }
        }
        context.write(new Text(trackId), new Text(maxLabel));
    }

    public static int calcDistance(String str1, String str2) {
        int[][] dp = new int[str1.length() + 1][str2.length() + 1];

        for (int i = 0; i <= str1.length(); i++) {
            for (int j = 0; j <= str2.length(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    int cost = (str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0 : 1;
                    dp[i][j] = Math.min(Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1), dp[i - 1][j - 1] + cost);
                }
            }
        }

        return dp[str1.length()][str2.length()];
    }

    public static class Pair {
        int distance;
        String label;

        public int getDistance() {
            return distance;
        }

        public String getLabel() {
            return label;
        }

        public Pair(int distance, String label) {
            this.distance = distance;
            this.label = label;
        }
    }
}
