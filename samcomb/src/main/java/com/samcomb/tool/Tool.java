package com.samcomb.tool;

import com.samcomb.utils.Rander;
import com.samcomb.utils.Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.util.*;

public class Tool {

    public static Config loadConfig(String configPath) {
        Config configFactory = ConfigFactory.parseFile(new File(configPath));
        // Logger.log("Config reloaded: " + configPath);
        return configFactory.resolve();
    }


    public static <T> T randomItem(List<T> arr) {
        int id = Rander.rand.nextInt(arr.size());
        return arr.get(id);
    }

    public static <T> List<T> randomItems(List<T> arr, int n) {
        List<Integer> ids = Utils.range(0, arr.size());
        Collections.shuffle(ids, new Random(Rander.rand.nextLong()));
        List<T> shuffledArr = new ArrayList<>();
        for (var id : ids) {
            shuffledArr.add(arr.get(id));
        }
        return shuffledArr;
    }

    public static double sum(Collection<Double> collection) {
        double s = 0.0;
        for (var d : collection) {
            s += d;
        }
        return s;
    }

    public static double mean(Collection<Double> collection) {
        double sum = sum(collection);
        return sum / (double) collection.size();
    }

    public static ArrayList<Integer> arange(int start, int end, int step) {
        var res = new ArrayList<Integer>();
        for (int i = start; i < end; i += step) {
            res.add(i);
        }
        return res;
    }

    public static double var(Collection<Double> collection) {
        double avg = mean(collection);
        double variance = 0.0;
        for (var d : collection) {
            variance += Math.pow(d - avg, 2.0);
        }
        variance = variance / (double) collection.size();
        return variance;
    }


}
