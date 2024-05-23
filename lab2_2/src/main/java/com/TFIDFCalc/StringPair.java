package com.TFIDFCalc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class StringPair implements WritableComparable<StringPair> {
    private String first;
    private String second;

    // Default constructor
    public StringPair() {
        this.first = "";
        this.second = "";
    }

    // Parameterized constructor
    public StringPair(String first, String second) {
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

    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }

    // Serialization method
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeUTF(second);
    }

    // Deserialization method
    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readUTF();
        second = in.readUTF();
    }

    // Compare method for sorting
    @Override
    public int compareTo(StringPair other) {
        // Compare the first strings first, then the second strings
        int cmp = first.compareTo(other.getFirst());
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(other.getSecond());
    }

    // Override toString for better readability
    @Override
    public String toString() {
        return "(" + first + ", " + second + ")";
    }
}
