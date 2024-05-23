//package com.TFIDFCalc;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//
//public class TextIntPair implements WritableComparable<TextIntPair> {
//    private Text first;
//    private IntWritable second;
//
//    // Default constructor
//    public TextIntPair() {
//        this.first = new Text();
//        this.second = new IntWritable();
//    }
//
//    // Parameterized constructor
//    public TextIntPair(Text first, IntWritable second) {
//        this.first = first;
//        this.second = second;
//    }
//
//    // Getters and setters
//    public Text getFirst() {
//        return first;
//    }
//
//    public void setFirst(Text first) {
//        this.first = first;
//    }
//
//    public IntWritable getSecond() {
//        return second;
//    }
//
//    public void setSecond(IntWritable second) {
//        this.second = second;
//    }
//
//    // Serialization method
//    @Override
//    public void write(DataOutput out) throws IOException {
//        first.write(out);
//        second.write(out);
//    }
//
//    // Deserialization method
//    @Override
//    public void readFields(DataInput in) throws IOException {
//        first.readFields(in);
//        second.readFields(in);
//    }
//
//    // Compare method for sorting
//    @Override
//    public int compareTo(TextIntPair other) {
//        // Compare the first Text objects first, then the second IntWritable objects
//        int cmp = first.compareTo(other.getFirst());
//        if (cmp != 0) {
//            return cmp;
//        }
//        return second.compareTo(other.getSecond());
//    }
//
//    // Override toString for better readability
//    @Override
//    public String toString() {
//        return "(" + first.toString() + ", " + second.toString() + ")";
//    }
//}
