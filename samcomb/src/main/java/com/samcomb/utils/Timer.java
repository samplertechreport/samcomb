package com.samcomb.utils;

public class Timer {
    private long initTimeStamp = System.currentTimeMillis();
    private long startTime = System.currentTimeMillis();
    public double totalTime = 0.0;

    public void reset() {
        initTimeStamp = System.currentTimeMillis();
        startTime = System.currentTimeMillis();
        totalTime = 0.0;
    }

    public void resetStartTime() {
        this.startTime = System.currentTimeMillis();
    }

    public double getElapsedTime() {
        double passTime = (System.currentTimeMillis() - startTime) / 1000.0;
        startTime = System.currentTimeMillis();
        // totalTime += passTime;
        return passTime;
    }

    public double getTotalTime() {
        return (System.currentTimeMillis() - initTimeStamp) / 1000.0;
    }
}

