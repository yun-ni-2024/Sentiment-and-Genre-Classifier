package com.TFIDFCalc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StringIntPair implements WritableComparable<StringIntPair> {
    private String first;
    private int second;

    // Default constructor
    public StringIntPair() {
        this.first = "";
        this.second = 0;
    }

    // Parameterized constructor
    public StringIntPair(String first, int second) {
        this.first = first;
        this.second = second;
    }

    // Getters and setters
    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    // Serialization method
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeInt(second);
    }

    // Deserialization method
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readUTF();
        second = in.readInt();
    }

    // Compare method for sorting
    @Override
    public int compareTo(StringIntPair other) {
        // Compare the first String objects first, then the second int objects
        int cmp = first.compareTo(other.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        return Integer.compare(second, other.getSecond());
    }

    // Override toString for better readability
    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
