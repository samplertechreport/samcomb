package com.samcomb.dataframe;


import com.samcomb.utils.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class MathTool {

    public static int precision = 15;

    public static boolean equal(double a, double b) {
        double multiple = Math.pow(10, precision);
        return Math.round(a * multiple) == Math.round(b * multiple);
    }


    public static ArrayList<Double> divide(List<Double> a, List<Double> b) {
        assert a.size() == b.size() && a.size() > 0 : "input lists should be equal size and cannot be empty!";
        var res = new ArrayList<Double>();
        for (int i = 0; i < a.size(); i++) {
//            assert !MathTool.equal(b.get(i), 0.0) : "denominator cannot be 0";
            var d = a.get(i) / b.get(i);
            res.add(d);
        }

        return res;
    }


    public static ArrayList<Integer> arange(int start, int end, int step) {
        var res = new ArrayList<Integer>();
        for (int i = start; i < end; i += step) {
            res.add(i);
        }
        return res;
    }


    public static ArrayList<Double> arange(double start, double end, double step) {
        var res = new ArrayList<Double>();
        for (double i = start; i < end; i += step) {
            res.add(i);
        }
        return res;
    }


    public static ArrayList<Double> divide(List<Double> a, double b) {
        assert !MathTool.equal(b, 0.0) : "cannot divide by 0!";
        var res = new ArrayList<Double>();
        for (int i = 0; i < a.size(); i++) {
            // assert b.get(i) > 0.0 : "denominator must be larger than 0";
            var d = a.get(i) / b;
            res.add(d);
        }

        return res;
    }


    public static ArrayList<Double> multiply(List<? extends Number> a, List<? extends Number> b) {
        assert a.size() == b.size() && a.size() > 0 : "input list cannot be empty!";
        var res = new ArrayList<Double>();
        for (int i = 0; i < a.size(); i++) {
            //  assert !MathTool.equal(b.get(i), 0.0) : "denominator cannot be 0";
            var d = a.get(i).doubleValue() * b.get(i).doubleValue();
            res.add(d);
        }

        return res;
    }


    public static ArrayList<Double> multiply(List<Double> a, double b) {
        // assert !MathTool.equal(b, 0.0): "cannot divide by 0!";
        var res = new ArrayList<Double>();
        for (int i = 0; i < a.size(); i++) {
            // assert b.get(i) > 0.0 : "denominator must be larger than 0";
            var d = a.get(i) * b;
            res.add(d);
        }

        return res;
    }


    public static void printStat(Collection<? extends Number> series) throws IOException {
        var sum = sum(series);
        var mini = min(series);
        var maxi = max(series);
        var std = stdev(series);
        var len = series.size();
        var s = "--------------reset print stat--------------\n";
        s += String.format(" min:%.10f\n max:%.10f\n sum:%.10f\n std:%.10f\n size:%d\n", mini, maxi, sum, std, len);
        s += "--------------end print stat--------------";
        Logger.log(s);
    }

    public static double sum(Collection<? extends Number> series) {
        assert series.size() > 0;
        return series
                .stream()
                .mapToDouble(x -> ((Number) x).doubleValue())
                .sum();
    }

    public static long count(Collection<?> series) {
        return series.size();
    }


    public static double mean(Collection<? extends Number> series) {
        assert series.size() > 0;
        return series
                .stream()
                .mapToDouble(x -> ((Number) x).doubleValue())
                .average()
                .getAsDouble();
    }


    public static double stdev(Collection<? extends Number> series) {
        assert series.size() > 0;

        double sum1 = 0.0;
        double sum2 = 0.0;
        for (var t : series) {
            double data = t.doubleValue();
            sum1 += data;
            sum2 += data * data;
        }

        double n = series.size();
        double avg = sum1 / n;
        double variance = sum2 / n - avg * avg;

        assert variance >= 0.0;
        return Math.sqrt(variance);
    }


    public static double min(Collection<? extends Number> series) {
        assert series.size() > 0;
        return series
                .stream()
                .mapToDouble(x -> ((Number) x).doubleValue())
                .min()
                .getAsDouble();
    }


    public static double max(Collection<? extends Number> series) {
        assert series.size() > 0;
        return series
                .stream()
                .mapToDouble(x -> ((Number) x).doubleValue())
                .max()
                .getAsDouble();
    }


}
