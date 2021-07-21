package com.samcomb.sampler;

import java.io.IOException;

public interface EstimatorInterface {
    public String uid = "";   //uid is used for identify the estimator when record the result.

    public long getRandseed();

    public void setRandseed(long randseed);

    public double getEstimation();

    public double getVariance();

    public double getCi();

    public String getName();

    public void query(int budget) throws IOException, Exception;
}
