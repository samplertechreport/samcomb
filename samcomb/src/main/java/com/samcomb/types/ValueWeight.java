package com.samcomb.types;

// used for median query processing
public class ValueWeight implements Comparable<ValueWeight> {
    public double value;
    public double weight;

    public ValueWeight(double value, double weight) {
        this.value = value;
        this.weight = weight;
    }

    @Override
    public int compareTo(ValueWeight o) {
        return Double.compare(this.value, o.value);
    }
}
