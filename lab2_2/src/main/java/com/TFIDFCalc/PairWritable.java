//package com.TFIDFCalc;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//import org.apache.hadoop.io.*;
//
//public class PairWritable<T1 extends WritableComparable<? super T1>, T2 extends WritableComparable<? super T2>> implements WritableComparable<PairWritable<T1, T2>> {
//
//    private T1 first;
//    private T2 second;
//
//    public PairWritable() {
//        this.first = null;
//        this.second = null;
//    }
//
//    public PairWritable(T1 first, T2 second) {
//        this.first = first;
//        this.second = second;
//    }
//
//    public T1 getFirst() {
//        return first;
//    }
//
//    public void setFirst(T1 first) {
//        this.first = first;
//    }
//
//    public T2 getSecond() {
//        return second;
//    }
//
//    public void setSecond(T2 second) {
//        this.second = second;
//    }
//
//    @Override
//    public void write(DataOutput out) throws IOException {
//        first.write(out);
//        second.write(out);
//    }
//
//    @Override
//    public void readFields(DataInput in) throws IOException {
////        if (first == null || second == null) {
////            throw new IOException("First or second is null");
////        }
//        first.readFields(in);
//        second.readFields(in);
//    }
//
//    @Override
//    public int compareTo(PairWritable<T1, T2> other) {
//        int firstComparison = this.first.compareTo(other.first);
//        if (firstComparison != 0) {
//            return firstComparison;
//        }
//        return this.second.compareTo(other.second);
//    }
//
//    @Override
//    public String toString() {
//        return "(" + first.toString() + ", " + second.toString() + ")";
//    }
//}
