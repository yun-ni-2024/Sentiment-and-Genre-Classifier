package com.Classification;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexPair implements WritableComparable<IndexPair> {
    private int centerId;
    private int pointId;

    // Default constructor (required for deserialization)
    public IndexPair() {}

    // Constructor
    public IndexPair(int centerId, int pointId) {
        this.centerId = centerId;
        this.pointId = pointId;
    }

    // Getters and Setters
    public int getCenterId() {
        return centerId;
    }

    public void setCenterId(int centerId) {
        this.centerId = centerId;
    }

    public int getPointId() {
        return pointId;
    }

    public void setPointId(int pointId) {
        this.pointId = pointId;
    }

    // Serialization
    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, centerId);
        WritableUtils.writeVInt(out, pointId);
    }

    // Deserialization
    @Override
    public void readFields(DataInput in) throws IOException {
        centerId = WritableUtils.readVInt(in);
        pointId = WritableUtils.readVInt(in);
    }

    // Comparison logic
    @Override
    public int compareTo(IndexPair o) {
        int result = Integer.compare(this.centerId, o.centerId);
        if (result == 0) {
            result = Integer.compare(this.pointId, o.pointId);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexPair intPair = (IndexPair) o;
        return centerId == intPair.centerId && pointId == intPair.pointId;
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(centerId);
        result = 31 * result + Integer.hashCode(pointId);
        return result;
    }

    @Override
    public String toString() {
        return centerId + "\t" + pointId;
    }
}
