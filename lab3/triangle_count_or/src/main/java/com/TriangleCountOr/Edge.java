package com.TriangleCountOr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Edge implements WritableComparable<Edge> {
    private boolean dir; // Direction of the edge, true means outgoing
    private String vertex; // Vertex linked to the edge

    public Edge() {
    }

    public Edge(boolean dir, String vertex) {
        this.dir = dir;
        this.vertex = vertex;
    }

    // Whether this edge is outgoing
    public boolean isOut() {
        return dir;
    }

    // Get the vertex linked to the edge
    public String getVertex() {
        return vertex;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBoolean(dir);
        WritableUtils.writeString(out, vertex);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dir = in.readBoolean();
        vertex = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(Edge other) {
        int cmp = Boolean.compare(this.dir, other.dir);

        if (cmp != 0) {
            return cmp;
        }

        return this.vertex.compareTo(other.vertex);
    }

    @Override
    public String toString() {
        return dir + "\t" + vertex;
    }

    @Override
    public int hashCode() {
        return (dir ? 1 : 0) * 31 + vertex.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        Edge otherEdge = (Edge) other;

        if (dir != otherEdge.dir) {
            return false;
        }

        return vertex.equals(otherEdge.vertex);
    }
}
