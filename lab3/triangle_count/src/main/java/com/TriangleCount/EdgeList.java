package com.TriangleCount;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

public class EdgeList implements WritableComparable<EdgeList> {
    private String vertex;
    private ArrayList<String> edge; // adjacent list of vertex

    public EdgeList() {
    }

    public EdgeList(String vertex, ArrayList<String> edge) {
        this.vertex = vertex;
        this.edge = edge;
    }

    public String getVertex() {
        return vertex;
    }

    public ArrayList<String> getEdges() {
        return edge;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(vertex);
        out.writeInt(edge.size());
        for (String v : edge) {
            out.writeUTF(v);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertex = in.readUTF();
        int size = in.readInt();
        edge = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            edge.add(in.readUTF());
        }
    }

    @Override
    public int compareTo(EdgeList other) {
        int result = this.vertex.compareTo(other.vertex);
        if (result == 0) {
            // Compare the size of the edges list
            result = Integer.compare(this.edge.size(), other.edge.size());
            if (result == 0) {
                // Compare the individual edges lexicographically if sizes are the same
                for (int i = 0; i < this.edge.size(); i++) {
                    result = this.edge.get(i).compareTo(other.edge.get(i));
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
        return vertex + "\t" + edge.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertex, edge);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        EdgeList otherEdge = (EdgeList) other;
        return Objects.equals(vertex, otherEdge.vertex) && Objects.equals(edge, otherEdge.edge);
    }
}
