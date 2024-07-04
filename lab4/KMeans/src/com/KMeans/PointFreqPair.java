package com.KMeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class PointFreqPair implements Writable {
    private double[] point;
    private int freq;

    public PointFreqPair() {
    }

    public PointFreqPair(double[] point, int freq) {
        this.point = point;
        this.freq = freq;
    }

    public double[] getPoint() {
        return point;
    }

    public int getFreq() {
        return freq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(point.length); // Write the length of the point array
        for (double val : point) {
            out.writeDouble(val); // Write each double value of the point
        }
        out.writeInt(freq); // Write the frequency
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt(); // Read the length of the point array
        point = new double[size];
        for (int i = 0; i < size; i++) {
            point[i] = in.readDouble(); // Read each double value of the point
        }
        freq = in.readInt(); // Read the frequency
    }

    @Override
    public String toString() {
        return Arrays.toString(point) + "\t" + freq;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(point), freq);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        PointFreqPair that = (PointFreqPair) other;
        return Arrays.equals(point, that.point) && freq == that.freq;
    }
}
