package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.dataframe.MathTool;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;

public class CompareTwoStepCombAndBlinkDbConfig {

    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2", "loan"));
        // config.add("schemaNames", Arrays.asList("skew_s1_z2"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("usePrecomputeSample", DefaultExpConfig.defaultUsePrecomputeSample);
            config.getConfig(schema).add("queryPath", DefaultExpConfig.defaultQueryPaths.get(schema));
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("queryAggregateType", AggregateType.SUM);
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultSamplers.get(schema + ".types"));
        }

        return config;
    }


    public static ArrayList<Strategy> getStrategies(Config expConfig) {
        var strategies = new ArrayList<Strategy>();
        int adaptiveInitSize = (int) DefaultStrategyConfig.getDefaultAdaptiveLcb().getPar("initSize");
        int budget = (int) DefaultStrategyConfig.getDefaultAdaptiveLcb().getPar("totalBudget");
        var weightAllocStrategyList = Arrays.asList("propToSelRows", "propToSize", "realOptimalAlloc", "pseudoOptimal", "mostSampleOnly");

        for (var weightAlloc : weightAllocStrategyList) {
            var adaptiveLcb = DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
            adaptiveLcb.addPar("initSizeRate", adaptiveInitSize / (double) budget);
            adaptiveLcb.addPar("weightAllocationStrategy", weightAlloc);
            strategies.add(adaptiveLcb);
        }

        ArrayList<Integer> initSizeList = new ArrayList<>();
        initSizeList.add(adaptiveInitSize);
        var initRate = MathTool.arange(0.01, 1.001, 0.01);
        for (var rate : initRate) {
            int size = (int) (budget * rate);
            initSizeList.add(size);
        }

        for (int initSize : initSizeList) {
            for (var weightAlloc : weightAllocStrategyList) {
//
//                var adaptiveLcb = DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
//                adaptiveLcb.addPar("initSize", initSize);
//                adaptiveLcb.addPar("initSizeRate", initSize / (double) budget);
//                adaptiveLcb.addPar("weightAllocationStrategy", weightAlloc);
//                strategies.add(adaptiveLcb);

                var eps = DefaultStrategyConfig.getDefaultEps().copy();
                eps.addPar("initSize", initSize);
                eps.addPar("initSizeRate", initSize / (double) budget);
                eps.addPar("weightAllocationStrategy", weightAlloc);
                strategies.add(eps);

                var twoStep = DefaultStrategyConfig.getDefaultTwoStep().copy();
                twoStep.addPar("initSize", initSize);
                twoStep.addPar("initSizeRate", initSize / (double) budget);
                twoStep.addPar("weightAllocationStrategy", weightAlloc);
                strategies.add(twoStep);
            }

            var blinkDb = DefaultStrategyConfig.getDefaultBlinkDb().copy();
            blinkDb.addPar("initSize", initSize);
            blinkDb.addPar("initSizeRate", initSize / (double) budget);
            strategies.add(blinkDb);
        }
        return strategies;
    }

//    public static ArrayList<Strategy> getStrategies(Config expConfig)
//    {
//
//        var strategies = new ArrayList<Strategy>();
//        int adaptiveInitSize = (int) DefaultStrategyConfig.defaultInitSize;
//        int budget = (int) DefaultStrategyConfig.defaultTotalBudget;
//
//        var adaptiveLcb = DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
//        adaptiveLcb.addPar("initSizeRate", adaptiveInitSize / (double) budget);
//        strategies.add(adaptiveLcb);
//
//
//        ArrayList<Integer> initSizeList = new ArrayList<>();
//        initSizeList.add(adaptiveInitSize);
//        var initRate = MathTool.arange(0.01, 1.001, 0.01);
//        for (var rate: initRate)
//        {
//            int size = (int) (budget*rate);
//            initSizeList.add(size);
//        }
//
//        var defaultBlinkDBStrategy = new Strategy();
//        defaultBlinkDBStrategy.name = StrategyName.BlinkDB;
//        defaultBlinkDBStrategy.addPar("totalBudget", DefaultStrategyConfig.defaultTotalBudget);
//        defaultBlinkDBStrategy.addPar("initSize", DefaultStrategyConfig.defaultInitSize);
//        defaultBlinkDBStrategy.addPar("recordIterState", DefaultStrategyConfig.defaultRecordIterState);
//
//        for (int initSize: initSizeList)
//        {
//
//            var eps = DefaultStrategyConfig.getDefaultEps().copy();
//            eps.addPar("initSize", initSize);
//            eps.addPar("initSizeRate", initSize / (double) budget);
//            strategies.add(eps);
//
//            var twoStep = DefaultStrategyConfig.getDefaultTwoStep().copy();
//            twoStep.addPar("initSize", initSize);
//            twoStep.addPar("initSizeRate", initSize / (double) budget);
//            strategies.add(twoStep);
//
//            var blinkDb = defaultBlinkDBStrategy.copy();
//            blinkDb.addPar("initSize", initSize);
//            blinkDb.addPar("initSizeRate", initSize/(double) budget);
//            strategies.add(blinkDb);
//        }
//        return strategies;
//    }

}

