package com.TriangleCount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class StringListPair implements WritableComparable<StringListPair> {
    private String ver;
    private ArrayList<String> list;

    // Default constructor
    public StringListPair() {
        this.ver = "";
        this.list = new ArrayList<>();
    }

    // Parameterized constructor
    public StringListPair(String ver, ArrayList<String> list) {
        this.ver = ver;
        this.list = list;
    }

    // Getter for ver
    public String getVer() {
        return ver;
    }

    // Setter for ver
    public void setVer(String ver) {
        this.ver = ver;
    }

    // Getter for list
    public ArrayList<String> getList() {
        return list;
    }

    // Setter for list
    public void setList(ArrayList<String> list) {
        this.list = list;
    }

    // Add a string to the list
    public void addToList(String item) {
        this.list.add(item);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, ver);
        out.writeInt(list.size());
        for (String item : list) {
            WritableUtils.writeString(out, item);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.ver = WritableUtils.readString(in);
        int size = in.readInt();
        this.list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(WritableUtils.readString(in));
        }
    }

    @Override
    public int compareTo(StringListPair o) {
        int result = this.ver.compareTo(o.getVer());
        if (result == 0) {
            int thisSize = this.list.size();
            int otherSize = o.getList().size();
            result = Integer.compare(thisSize, otherSize);
            if (result == 0) {
                for (int i = 0; i < thisSize; i++) {
                    result = this.list.get(i).compareTo(o.getList().get(i));
                    if (result != 0) {
                        break;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "VerListPairWritable{" +
                "ver='" + ver + '\'' +
                ", list=" + list +
                '}';
    }
}
