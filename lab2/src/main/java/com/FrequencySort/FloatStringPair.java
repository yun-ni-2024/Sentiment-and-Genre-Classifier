package com.FrequencySort;

public class FloatStringPair implements Comparable<FloatStringPair> {
    private float floatValue;
    private String stringValue;

    public FloatStringPair(float floatValue, String stringValue) {
        this.floatValue = floatValue;
        this.stringValue = stringValue;
    }

    public float getFloatValue() {
        return floatValue;
    }

    public String getStringValue() {
        return stringValue;
    }

    @Override
    public int compareTo(FloatStringPair other) {
        // Compare based on the float value
        return Float.compare(this.floatValue, other.getFloatValue());
    }
}
