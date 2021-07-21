package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

public class CompareLcbEpsConfig {
    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2", "loan"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("queryAggregateType", AggregateType.SUM);
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("usePrecomputeSample", DefaultExpConfig.defaultUsePrecomputeSample);
            config.getConfig(schema).add("queryPath", DefaultExpConfig.defaultQueryPaths.get(schema));
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultSamplers.get(schema + ".types"));
        }

        return config;
    }


//    public static ArrayList<Strategy> getStrategies(Config expConfig) {
//
//        var strategies = new ArrayList<Strategy>();
//
//        var eps = DefaultStrategyConfig.getDefaultEps().copy();
//        eps.addPar("recordIterState", true);
//        strategies.add(eps);
//
//        var lcb = DefaultStrategyConfig.getDefaultLcb().copy();
//        lcb.addPar("recordIterState", true);
//        strategies.add(lcb);
//
//
//        var adaptiveLcb=  DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
//        adaptiveLcb.addPar("recordIterState", true);
//        strategies.add(adaptiveLcb);
//        return strategies;
//    }

    public static ArrayList<Strategy> getStrategies(Config expConfig) {

        var rander = new Random("LcbEps".hashCode());

        var strategies = new ArrayList<Strategy>();

        int maxBudget = DefaultStrategyConfig.defaultTotalBudget;
        var budgetList = new HashSet<Integer>();
        for (int i = 10000; i < maxBudget; i += 2000) {
            budgetList.add(i);
        }

        var lcb = DefaultStrategyConfig.getDefaultLcb().copy();
        lcb.addPar("totalBudget", maxBudget);
        lcb.addPar("recordIterState", true);
        lcb.addPar("recordIterState.budgetList", budgetList);
        strategies.add(lcb);

        var eps = DefaultStrategyConfig.getDefaultEps().copy();
        eps.addPar("decayConstant", rander.nextInt(300) + 1);
        eps.addPar("totalBudget", maxBudget);
        eps.addPar("recordIterState", true);
        eps.addPar("recordIterState.budgetList", budgetList);
        strategies.add(eps);

        var adaptiveLcb = DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
        adaptiveLcb.addPar("totalBudget", maxBudget);
        adaptiveLcb.addPar("recordIterState", true);
        adaptiveLcb.addPar("recordIterState.budgetList", budgetList);
        strategies.add(adaptiveLcb);

        return strategies;
    }

}
