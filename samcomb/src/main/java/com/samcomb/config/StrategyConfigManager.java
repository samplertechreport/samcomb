package com.samcomb.config;

import com.samcomb.allocation.Strategy;
import com.samcomb.allocation.StrategyName;
import com.samcomb.config.expconfig.CompareOfflineOptimalAndOnlineOptimalConfig;
import com.samcomb.dataframe.MathTool;
import com.samcomb.experiment.exp.ExpName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class StrategyConfigManager {


    public static ArrayList<Strategy> getStrategies(ExpName expName, List<String> samplerColumns, List<String> samplerTypes, String schemaName) throws Exception {

        // int totalBudget = 144000;
        // int initSize = 600;
        // int batchSize = 200;

        var expConfig = ExpConfigManager.getExpConfig(expName);

        int totalBudget;
        switch (schemaName) {
            case "loan": {
                totalBudget = 28751;
                break;
            }
            case "skew_s05_z2": {
                totalBudget = 30000;
                break;
            }
            case "skew_s1_z2": {
                totalBudget = 60000;
                break;
            }
            default:
                if (schemaName.startsWith("skew_s1_z2_")) {
                    totalBudget = 60000;
                } else {
                    throw new IllegalStateException("unknown totalBudget for dataset:" + schemaName);
                }
        }


        String defaultWeightAlloc = "propToSize";

        var defaultEpsStrategy = new Strategy();
        defaultEpsStrategy.name = StrategyName.Eps;
        defaultEpsStrategy.addPar("totalBudget", totalBudget);
        defaultEpsStrategy.addPar("initSize", 600); //should be batch size* samplers
        defaultEpsStrategy.addPar("batchType", "constant");
        defaultEpsStrategy.addPar("batchSize", 200);
        defaultEpsStrategy.addPar("decayConstant", 10.0);
        defaultEpsStrategy.addPar("recordIterState", false);
        defaultEpsStrategy.addPar("weightAllocationStrategy", defaultWeightAlloc);


        var defaultRandomAllocStrategy = new Strategy();
        defaultRandomAllocStrategy.name = StrategyName.Random;
        defaultRandomAllocStrategy.addPar("totalBudget", totalBudget);
        defaultRandomAllocStrategy.addPar("initSize", 600);
        defaultRandomAllocStrategy.addPar("batchType", "constant");
        defaultRandomAllocStrategy.addPar("batchSize", 200);
        defaultRandomAllocStrategy.addPar("recordIterState", false);
        defaultRandomAllocStrategy.addPar("weightAllocationStrategy", defaultWeightAlloc);


        var defaultEqualAllocStrategy = new Strategy();
        defaultEqualAllocStrategy.name = StrategyName.Equal;
        defaultEqualAllocStrategy.addPar("totalBudget", totalBudget);
        defaultEqualAllocStrategy.addPar("recordIterState", false);
        defaultEqualAllocStrategy.addPar("weightAllocationStrategy", defaultWeightAlloc);


        var defaultTwoStepStrategy = new Strategy();
        defaultTwoStepStrategy.name = StrategyName.TwoStepComb;
        defaultTwoStepStrategy.addPar("totalBudget", totalBudget);
        defaultTwoStepStrategy.addPar("initSize", 600);
        defaultTwoStepStrategy.addPar("recordIterState", false);
        defaultTwoStepStrategy.addPar("weightAllocationStrategy", defaultWeightAlloc);


        var defaultBlinkDBStrategy = new Strategy();
        defaultBlinkDBStrategy.name = StrategyName.BlinkDB;
        defaultBlinkDBStrategy.addPar("totalBudget", totalBudget);
        defaultBlinkDBStrategy.addPar("initSize", 600);
        defaultBlinkDBStrategy.addPar("recordIterState", false);


        var defaultSingleSamplerStrategy = new Strategy();
        defaultSingleSamplerStrategy.name = StrategyName.SingleSampler;
        defaultSingleSamplerStrategy.addPar("totalBudget", totalBudget);
        defaultSingleSamplerStrategy.addPar("batchType", "constant");
        defaultSingleSamplerStrategy.addPar("batchSize", 200);
        defaultSingleSamplerStrategy.addPar("recordIterState", false);


        // offline optimal means the real optimal sampler in all samplers.
        var defaultOfflineOptimalStrategy = new Strategy();
        defaultOfflineOptimalStrategy.name = StrategyName.OfflineOptimal;
        defaultOfflineOptimalStrategy.addPar("totalBudget", totalBudget);
        defaultOfflineOptimalStrategy.addPar("batchType", "constant");
        defaultOfflineOptimalStrategy.addPar("batchSize", 200);
        defaultOfflineOptimalStrategy.addPar("recordIterState", false);


        // online optimal means the real optimal sampler in the available samplers.
        var defaultOnlineOptimalStrategy = new Strategy();
        defaultOnlineOptimalStrategy.name = StrategyName.OnlineOptimal;
        defaultOnlineOptimalStrategy.addPar("totalBudget", totalBudget);
        defaultOnlineOptimalStrategy.addPar("batchType", "constant");
        defaultOnlineOptimalStrategy.addPar("batchSize", 200);
        defaultOnlineOptimalStrategy.addPar("recordIterState", false);


        var defaultLcbStrategy = new Strategy();
        defaultLcbStrategy.name = StrategyName.Lcb;
        defaultLcbStrategy.addPar("totalBudget", totalBudget);
        defaultLcbStrategy.addPar("initSize", 600);
        defaultLcbStrategy.addPar("batchType", "constant");
        defaultLcbStrategy.addPar("batchSize", 200);
        defaultLcbStrategy.addPar("lcbWeight", 1.0);
        defaultLcbStrategy.addPar("boundType", "hoeffding");
        defaultLcbStrategy.addPar("recordIterState", false);
        defaultLcbStrategy.addPar("weightAllocationStrategy", defaultWeightAlloc);


        var defaultIdealLcbStrategy = defaultLcbStrategy.copy();
        defaultIdealLcbStrategy.name = StrategyName.IdealLcb;

        var defaultAdaptiveLcbStrategy = defaultLcbStrategy.copy();
        defaultAdaptiveLcbStrategy.name = StrategyName.AdaptiveLcb;


        var strategies = new ArrayList<Strategy>();

        switch (expName) {

            case onlineAggregation: {
                //   var paraRatio = MathTool.arange(0.01, 1.001, 0.01);

                var adaptiveLcb = defaultAdaptiveLcbStrategy.copy();
                strategies.add(adaptiveLcb);
                adaptiveLcb.addPar("recordIterState", true);
                strategies.add(adaptiveLcb);

                int nsampler = samplerColumns.size();
                for (int i = 0; i < nsampler; i++) {
                    var samplerName = samplerTypes.get(i) + "_" + samplerColumns.get(i);
                    var singleStrategy = defaultSingleSamplerStrategy.copy();
                    singleStrategy.addPar("singleSamplerName", samplerName);
                    singleStrategy.addPar("recordIterState", true);
                    strategies.add(singleStrategy);
                }

                /*
                var ufStraregy = defaultSingleSamplerStrategy.copy();
                var ufsamplerName = "uf_None";
                ufStraregy.addPar("exp.paraRatio", rate);
                ufStraregy.addPar("totalBudget", curBudget);
                ufStraregy.addPar("singleSamplerName", ufsamplerName);
                ufStraregy.addPar("recordIterState", true);
                strategies.add(ufStraregy);
                */

                break;
            }

            case combineVsNotcombine: {
//                var epsStrategy = defaultEpsStrategy.copy();
//                strategies.add(epsStrategy);

                var adaptiveLcb = defaultAdaptiveLcbStrategy.copy();
                strategies.add(adaptiveLcb);

                int nsampler = samplerColumns.size();
                if (nsampler != samplerTypes.size())
                    throw new IllegalStateException("sampler column size != sampler type size!");
                for (int i = 0; i < nsampler; i++) {
                    var samplerName = samplerTypes.get(i) + "_" + samplerColumns.get(i);
                    var singleStrategy = defaultSingleSamplerStrategy.copy();
                    singleStrategy.addPar("singleSamplerName", samplerName);
                    strategies.add(singleStrategy);
                }

                break;
            }

            case compareRandomAndEqualAlloc: {
                var adaptiveLcbStrategy = defaultAdaptiveLcbStrategy.copy();
                // var epsStrategy = defaultEpsStrategy.copy();
                var randomAlloc = defaultRandomAllocStrategy.copy();
                var equalAlloc = defaultEqualAllocStrategy.copy();

                strategies.add(adaptiveLcbStrategy);
                strategies.add(randomAlloc);
                strategies.add(equalAlloc);

                break;
            }

            case compareOfflineOptimalAndOnlineOptimalSampler: {
                CompareOfflineOptimalAndOnlineOptimalConfig.getStrategies(expConfig);
            }


//            case compareTwoStepCombAndBlinkDb:
//            {
//                var adaptiveLcb = defaultAdaptiveLcbStrategy.copy();
//                strategies.add(adaptiveLcb);
//
//                int budget = (int) defaultTwoStepStrategy.getPar("totalBudget");
//                int smallInitSize = 2000;
//                int largeInitSize = (int) (budget * 0.9);
//
//                var smallInitTwoStep = defaultTwoStepStrategy.copy();
//                smallInitTwoStep.addPar("initSize", smallInitSize);
//                strategies.add(smallInitTwoStep);
//
//
//                var largeInitTwoStep = defaultTwoStepStrategy.copy();
//                largeInitTwoStep.addPar("initSize", largeInitSize);
//                strategies.add(largeInitTwoStep);
//                break;
//            }

            case compareTwoStepCombAndBlinkDb: {
                int adaptiveInitSize = (int) defaultAdaptiveLcbStrategy.getPar("initSize");
                int budget = (int) defaultAdaptiveLcbStrategy.getPar("totalBudget");
                var weightAllocStrategyList = Arrays.asList("propToSelRows", "propToSize", "realOptimalAlloc", "pseudoOptimal", "mostSampleOnly");

                for (var weightAlloc : weightAllocStrategyList) {
                    var adaptiveLcb = defaultAdaptiveLcbStrategy.copy();
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
                        var twoStep = defaultTwoStepStrategy.copy();
                        twoStep.addPar("initSize", initSize);
                        twoStep.addPar("initSizeRate", initSize / (double) budget);
                        twoStep.addPar("weightAllocationStrategy", weightAlloc);
                        strategies.add(twoStep);
                    }

                    var blinkDb = defaultBlinkDBStrategy.copy();
                    blinkDb.addPar("initSize", initSize);
                    blinkDb.addPar("initSizeRate", initSize / (double) budget);
                    strategies.add(blinkDb);
                }
                break;
            }

            case varyInitSize: {
                //var initSizeList = Arrays.asList(600, 900, 1200, 1800, 2400, 4800, 9600, 19200, 38400, 76800, 144000);

                ArrayList<Integer> initSizeList = new ArrayList<>();
                initSizeList.add(600);
                var initRate = MathTool.arange(0.05, 1.01, 0.05);

                for (var rate : initRate) {
                    int size = (int) (144000 * rate);
                    initSizeList.add(size);
                }
                // Logger.log(initSize);


                var epsStrategy = defaultEpsStrategy.copy();
                strategies.add(epsStrategy);

                for (var initSize : initSizeList) {
                    var twoStep = defaultTwoStepStrategy.copy();
                    twoStep.addPar("initSize", initSize);
                    strategies.add(twoStep);
                }
                break;
            }


            case varyC: {
                var cList = MathTool.arange(0, 300, 10);
                for (double c : cList) {
                    var eps = defaultEpsStrategy.copy();
                    eps.addPar("decayConstant", c);
                    eps.addPar("weightAllocationStrategy", "propToSelRows");
                    eps.addPar("recordIterState", true);
                    strategies.add(eps);
                }

                break;
            }

            case varyBatchSize: {
                var batchList = MathTool.arange(200, 2100, 200);
                int nsampler = samplerColumns.size();
                for (int batchSize : batchList) {
                    var eps = defaultEpsStrategy.copy();
                    eps.addPar("initSize", batchSize * nsampler);
                    eps.addPar("batchSize", batchSize);
                    eps.addPar("weightAllocationStrategy", "propToSelRows");
                    eps.addPar("recordIterState", true);
                    strategies.add(eps);
                }
                break;
            }


            case compareCInitsize: {
                var paraRatio = MathTool.arange(0.0, 1.001, 0.01);

                for (var rate : paraRatio) {
                    double epsC = (144000.0 - 600.0) / 200.0 * rate;
                    int initSize = Math.max(600, (int) (144000 * rate));

                    var epsStrategy = defaultEpsStrategy.copy();
                    epsStrategy.addPar("exp.paraRatio", rate);
                    epsStrategy.addPar("decayConstant", epsC);
                    strategies.add(epsStrategy);


                    var twoStep = defaultTwoStepStrategy.copy();
                    twoStep.addPar("exp.paraRatio", rate);
                    twoStep.addPar("initSize", initSize);
                    strategies.add(twoStep);
                }
                break;
            }


            case compareWeightAllocation: {
                var budgetList = MathTool.arange(1200, 144000, 2000);
                for (var budget : budgetList) {
                    var s1 = defaultEpsStrategy.copy();
                    s1.addPar("totalBudget", budget);
                    s1.addPar("weightAllocationStrategy", "pseudoOptimal");

                    var s2 = defaultEpsStrategy.copy();
                    s2.addPar("totalBudget", budget);
                    s2.addPar("weightAllocationStrategy", "propToSize");

                    strategies.add(s1);
                    strategies.add(s2);
                }
                break;
            }


            case compareLcbEps: {

                var eps = defaultEpsStrategy.copy();
                eps.addPar("recordIterState", true);
                eps.addPar("weightAllocationStrategy", "propToSelRows");

                var lcb = defaultLcbStrategy.copy();
                lcb.addPar("recordIterState", true);
                lcb.addPar("weightAllocationStrategy", "propToSelRows");


//                var idealLcb = defaultIdealLcbStrategy.copy();
//                idealLcb.addPar("recordIterState", true);
//                idealLcb.addPar("totalBudget", budget);
//                idealLcb.addPar("weightAllocationStrategy", "propToSelRows");

                var adaptiveLcb = defaultAdaptiveLcbStrategy.copy();
                adaptiveLcb.addPar("recordIterState", true);
                adaptiveLcb.addPar("weightAllocationStrategy", "propToSelRows");


//                var randomAlloc = defaultRandomAllocStrategy.copy();
//                randomAlloc.addPar("recordIterState", true);
//                randomAlloc.addPar("totalBudget", budget);
//                randomAlloc.addPar("weightAllocationStrategy", "propToSelRows");

                strategies.add(eps);
                strategies.add(lcb);
                //  strategies.add(idealLcb);
                strategies.add(adaptiveLcb);
                //   strategies.add(randomAlloc);

                break;
            }

            case selectivityEstimation: {

                var samplerArray = new ArrayList<String>();
                // -3 to avoid the OutOfMemoryError
                for (int i = 0; i < samplerTypes.size(); i++) {
                    var sname = samplerTypes.get(i) + "_" + samplerColumns.get(i);
                    samplerArray.add(sname);
                    var availableSamplers = new ArrayList<>(samplerArray);

                    var adaptiveLcb = defaultAdaptiveLcbStrategy.copy();
                    int batchSize = (int) adaptiveLcb.getPar("batchSize");
                    adaptiveLcb.addPar("initSize", batchSize * samplerTypes.size());
                    //  adaptiveLcb.addPar("recordIterState", true);
                    adaptiveLcb.addPar("availableSamplers", availableSamplers);
                    adaptiveLcb.addPar("samplerNum", availableSamplers.size());
                    strategies.add(adaptiveLcb);

                    var randomAlloc = defaultRandomAllocStrategy.copy();
                    randomAlloc.addPar("initSize", (int) adaptiveLcb.getPar("initSize"));
                    randomAlloc.addPar("availableSamplers", availableSamplers);
                    randomAlloc.addPar("samplerNum", availableSamplers.size());
                    ;
                    strategies.add(randomAlloc);

                    var equalAlloc = defaultEqualAllocStrategy.copy();
                    equalAlloc.addPar("availableSamplers", availableSamplers);
                    equalAlloc.addPar("samplerNum", availableSamplers.size());
                    strategies.add(equalAlloc);
                }
                break;
            }

            case varySampelerNum: {

                var samplerArray = new ArrayList<String>();
                for (int i = 0; i < samplerTypes.size(); i++) {
                    var sname = samplerTypes.get(i) + "_" + samplerColumns.get(i);
                    samplerArray.add(sname);
                    var availableSamplers = new ArrayList<>(samplerArray);

                    var adaptiveLcb = defaultAdaptiveLcbStrategy.copy();
                    int batchSize = (int) adaptiveLcb.getPar("batchSize");
                    adaptiveLcb.addPar("initSize", batchSize * samplerTypes.size());
                    //  adaptiveLcb.addPar("recordIterState", true);
                    adaptiveLcb.addPar("availableSamplers", availableSamplers);
                    adaptiveLcb.addPar("samplerNum", availableSamplers.size());
                    strategies.add(adaptiveLcb);
                }

                break;
            }


            case varyDataSize: {

                var samplerNames = Arrays.asList("uf_None", "sf_l_shipdate", "bk_l_extendedprice");
                var samplerArray = new ArrayList<String>();
                for (var sname : samplerNames) {
                    samplerArray.add(sname);
                    var availableSamplers = new ArrayList<>(samplerArray);

                    var eps = defaultEpsStrategy.copy();
                    int batchSize = (int) eps.getPar("batchSize");
                    eps.addPar("initSize", batchSize * samplerNames.size());
                    eps.addPar("recordIterState", false);
                    eps.addPar("availableSamplers", availableSamplers);
                    eps.addPar("samplerNum", availableSamplers.size());
                    strategies.add(eps);
                }

                break;
            }


            case varySampleSize: {

                var samplerNames = Arrays.asList("uf_None", "sf_l_shipdate", "bk_l_extendedprice");
                var budgetList = new HashSet<Integer>();
                for (var ratio : Arrays.asList(0.1, 0.2, 0.3, 0.4, 0.5)) {
                    budgetList.add((int) (6000000 * ratio));
                }

                int budget = (int) (6000000 * 0.5);
                var samplerArray = new ArrayList<String>();
                for (var sname : samplerNames) {
                    samplerArray.add(sname);
                    var availableSamplers = new ArrayList<>(samplerArray);
                    var eps = defaultEpsStrategy.copy();
                    int batchSize = (int) eps.getPar("batchSize");
                    eps.addPar("totalBudget", budget);
                    eps.addPar("initSize", batchSize * samplerNames.size());
                    eps.addPar("recordIterState", true);
                    eps.addPar("recordIterState.budgetList", budgetList);
                    eps.addPar("availableSamplers", availableSamplers);
                    eps.addPar("samplerNum", availableSamplers.size());
                    strategies.add(eps);
                }


                break;
            }


            case computeMaxQuery: {
                var samplerNames = Arrays.asList("uf_None", "sf_l_shipdate", "bk_l_extendedprice");
                var samplerArray = new ArrayList<String>();
                for (var sname : samplerNames) {
                    samplerArray.add(sname);
                    var availableSamplers = new ArrayList<>(samplerArray);

                    var eps = defaultEpsStrategy.copy();
                    int batchSize = (int) eps.getPar("batchSize");
                    eps.addPar("initSize", batchSize * samplerNames.size());
                    eps.addPar("recordIterState", true);
                    eps.addPar("availableSamplers", availableSamplers);
                    eps.addPar("samplerNum", availableSamplers.size());
                    strategies.add(eps);
                }
                break;
            }


            case computeMedianQuery: {
                var samplerNames = Arrays.asList("uf_None", "sf_l_shipdate", "bk_l_extendedprice");
                var samplerArray = new ArrayList<String>();
                for (var sname : samplerNames) {
                    samplerArray.add(sname);
                    var availableSamplers = new ArrayList<>(samplerArray);

                    var eps = defaultEpsStrategy.copy();
                    int batchSize = (int) eps.getPar("batchSize");
                    eps.addPar("initSize", batchSize * samplerNames.size());
                    eps.addPar("recordIterState", true);
                    eps.addPar("availableSamplers", availableSamplers);
                    eps.addPar("samplerNum", availableSamplers.size());
                    strategies.add(eps);
                }
                break;
            }

            case computeRealVarD: {
//                var columns = new String[] {"None", "l_orderkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_returnflag", "l_shipdate"};
//                var types = new String[] { "uf", "bk", "bk", "bk", "bk", "sf", "sf"};
                var columns = samplerColumns;
                var types = samplerTypes;
                for (int i = 0; i < columns.size(); i++) {
                    var samplerName = types.get(i) + "_" + columns.get(i);
                    var single = defaultSingleSamplerStrategy.copy();
                    single.addPar("singleSamplerName", samplerName);
                    strategies.add(single);
                }

                break;
            }

            default:
                throw new IllegalArgumentException("unknow exp name:" + expName);
        }


        return strategies;


    }

}
