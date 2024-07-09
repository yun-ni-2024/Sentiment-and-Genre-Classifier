package com.KNN;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class KNNMapper extends Mapper<Object, Text, Text, Text> {
    Map<String, String> sentimentMap = new HashMap<>();
    Map<String, String> attrMap = new HashMap<>();
    Set<String> trackIdSet;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] cacheFiles = context.getLocalCacheFiles();

        if (cacheFiles != null && cacheFiles.length > 0) {
            // train path
            Path path1 = cacheFiles[0];
            BufferedReader reader1 = new BufferedReader(new FileReader(path1.toString()));
            String line1;
            while ((line1 = reader1.readLine()) != null)
            {
                String[] parts = line1.split(",");
                String trackId = parts[0];
                String sentiment = parts[1];
                sentimentMap.put(trackId, sentiment);
            }
            reader1.close();

            // lyrics path
            for (int i = 1; i < cacheFiles.length; ++i) {
                Path path2 = cacheFiles[i];
                BufferedReader reader2 = new BufferedReader(new FileReader(path2.toString()));
                String line2;
                while ((line2 = reader2.readLine()) != null) {
                    if (line2.startsWith("#") || line2.startsWith("%")) {
                        continue;
                    }
                    String[] parts = line2.split(",");
                    String trackId = parts[0];
                    String attr = parts[2];
                    attrMap.put(trackId, attr);
                }
                reader2.close();
            }
        }

        Set<String> keys1 = sentimentMap.keySet();
        Set<String> keys2 = attrMap.keySet();
        trackIdSet = new HashSet<>(keys1);
        trackIdSet.retainAll(keys2);
    }

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("#") || line.startsWith("%")) {
            return;
        }

        String[] parts = line.split(",");
        int[] attr = new int[6000];
        Arrays.fill(attr, 0);
        for (int i = 2; i < parts.length; ++i) {
            String[] pair = parts[i].split(":");
            int attrInd = Integer.parseInt(pair[0]);
            int attrVal = Integer.parseInt(pair[1]);
            attr[attrInd] = attrVal;
        }

        int k = 5;
        PriorityQueue<Pair> maxHeap = new PriorityQueue<>(k, new Comparator<Pair>() {
            public int compare(Pair a, Pair b) {
                return Integer.compare(b.distance, a.distance); // 最大堆，比较距离的逆序
            }
        });

        int[] sampleAttr = new int[6000];
        for (String sampleTrackId : trackIdSet) {
            String sampleLabel = sentimentMap.get(sampleTrackId);
            String sampleAttrStr = attrMap.get(sampleTrackId);
            String[] sampleParts = sampleAttrStr.split(",");
            Arrays.fill(sampleAttr, 0);
            for (int i = 0; i < sampleParts.length; ++i) {
                String[] pair = sampleParts[i].split(":");
                int attrInd = Integer.parseInt(pair[0]);
                int attrVal = Integer.parseInt(pair[1]);
                sampleAttr[attrInd] = attrVal;
            }

            int dis = 0;
            for (int i = 1; i <= 5000; ++i) {
                dis += Math.pow((attr[i] - sampleAttr[i]), 2);
                Pair pair = new Pair(dis, sampleLabel);
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
        context.write(key, new Text(maxLabel));
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
