package com.samcomb.sampler;

public class EstimatorInfo {
    public double ds;
    public double realResult;
    public double realVarz;
    // public double realPredSelectRows;

    public EstimatorInfo setDs(double ds) {
        this.ds = ds;
        return this;
    }

    public EstimatorInfo setRealResult(double realResult) {
        this.realResult = realResult;
        return this;
    }

    public EstimatorInfo setRealVarz(double realVarz) {
        this.realVarz = realVarz;
        return this;
    }

//    public EstimatorInfo setRealPredSelectRows(double realPredSelectRows) {
//        this.realPredSelectRows = realPredSelectRows;
//        return this;
//    }
}
