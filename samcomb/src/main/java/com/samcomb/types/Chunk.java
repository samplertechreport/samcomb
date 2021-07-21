package com.samcomb.types;


import java.util.ArrayList;

public class Chunk {
    public long tupleNum = 0;
    public double sum = 0;
    public double sqrsum = 0;
    public double estimatedVarz = 0;
    public ArrayList<Double> zValues;

    public Chunk(ArrayList<Double> zValues) {
        assert zValues.size() > 1;
        this.zValues = zValues;

        this.sum = 0;
        this.sqrsum = 0;
        for (double d : zValues) {
            this.sum += d;
            this.sqrsum += d * d;
        }
        int c = zValues.size();
        this.estimatedVarz = this.sqrsum / (c - 1.0) - (this.sum * this.sum) / (c * (c - 1.0));
        this.tupleNum = c;
    }

    @Override
    public String toString() {
        return String.format("sum:%f, sqrsum:%f, estimatedVarz:%f, zValues:%s", sum, sqrsum, estimatedVarz, zValues.toString());
    }


}
