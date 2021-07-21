package com.samcomb.sampler;

import com.samcomb.dataframe.MathTool;
import com.samcomb.utils.Logger;

import java.io.IOException;
import java.util.*;

public class SampleDrawer<T> {
    public List<T> data;
    public List<Double> prob;
    public List<Double> aggProb;
    public long randSeed;
    public Random rand;

    public static void test() throws IOException {
        var data = Arrays.asList(1.0, 2.0, 3.0, 4.0);
        var prob = Arrays.asList(0.1, 0.2, 0.3, 0.4);
        var a = new SampleDrawer(data, prob, 0);
        var randSample = a.drawSample(10, true);
        Logger.log(randSample);
    }


    private static boolean checkProb(List<Double> prob) throws IOException {
        assert !prob.isEmpty();
        double s = 0.0;
        for (var p : prob) {
            if (p <= 0.0) return false;
            s += p;
        }
        Logger.log("s=" + s);
        return MathTool.equal(s, 1.0);
    }


    private void setAggProb(List<Double> prob) {
        assert !prob.isEmpty();
        var aggProb = new ArrayList<Double>();
        aggProb.add(prob.get(0));
        for (int i = 1; i < prob.size(); i++) {
            var aggValue = aggProb.get(i - 1) + prob.get(i);
            aggProb.add(aggValue);
        }
        this.aggProb = aggProb;
    }

    public SampleDrawer(List<T> data, List<Double> prob, long randSeed) throws IOException {
        assert data.size() == prob.size() && data.size() > 0;
        assert checkProb(prob);
        this.data = data;
        this.prob = prob;
        this.randSeed = randSeed;
        this.rand = new Random(randSeed);
        this.setAggProb(prob);
    }

    public void reset() {
        this.rand = new Random(randSeed);
    }

    public void setRandSeed(long randSeed) {
        this.randSeed = randSeed;
        this.rand = new Random(randSeed);
    }

    public ArrayList<T> drawSample(int size, boolean withReplacement) throws IOException {

        //TODO: withReplacement = false.
        assert size > 0;
        assert withReplacement;
        var selectData = new ArrayList<T>();
        for (int i = 0; i < size; i++) {
            var randValue = rand.nextDouble();
            var ind = Collections.binarySearch(aggProb, randValue);
            int rightIndex = -ind - 1;
            if (rightIndex < 0) rightIndex = ind;
            selectData.add(data.get(rightIndex));
        }
        return selectData;
    }


    public ArrayList<Integer> drawSampleIndex(int size, boolean withReplacement) {

        //TODO: withReplacement = false.
        assert size > 0;
        assert withReplacement;
        var selectData = new ArrayList<Integer>();
        for (int i = 0; i < size; i++) {
            var randValue = rand.nextDouble();
            var ind = Collections.binarySearch(aggProb, randValue);
            int rightIndex = -ind - 1;
            if (rightIndex < 0) rightIndex = ind;
            selectData.add(rightIndex);
        }
        return selectData;
    }

}
