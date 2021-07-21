package com.samcomb.config;

import com.samcomb.allocation.Strategy;
import com.samcomb.allocation.StrategyName;

public class DefaultStrategyConfig {
    // public static final int defaultTotalBudget = 60000;
    public static final int defaultTotalBudget = 144000;
    public static final int defaultInitSize = 600;
    // public static final int defaultInitSize = 600;
    public static final int defaultBatchSize = 200;
    public static final double defaultDecayConstant = 10.0;
    public static final String defaultWeightAllocationStrategy = "propToSelRows";
    public static final String defaultBatchType = "constant";
    public static final boolean defaultRecordIterState = false;

    public static Strategy getDefaultEps() {
        var defaultEpsStrategy = new Strategy();
        defaultEpsStrategy.name = StrategyName.Eps;
        defaultEpsStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultEpsStrategy.addPar("initSize", defaultInitSize); //should be batch size* samplers
        defaultEpsStrategy.addPar("batchType", defaultBatchType);
        defaultEpsStrategy.addPar("batchSize", defaultBatchSize);
        defaultEpsStrategy.addPar("decayConstant", defaultDecayConstant);
        defaultEpsStrategy.addPar("recordIterState", defaultRecordIterState);
        defaultEpsStrategy.addPar("weightAllocationStrategy", defaultWeightAllocationStrategy);
        return defaultEpsStrategy;
    }

    public static Strategy getDefaultRandomAlloc() {
        var defaultRandomAllocStrategy = new Strategy();
        defaultRandomAllocStrategy.name = StrategyName.Random;
        defaultRandomAllocStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultRandomAllocStrategy.addPar("initSize", defaultInitSize);
        defaultRandomAllocStrategy.addPar("batchType", defaultBatchType);
        defaultRandomAllocStrategy.addPar("batchSize", defaultBatchSize);
        defaultRandomAllocStrategy.addPar("recordIterState", defaultRecordIterState);
        defaultRandomAllocStrategy.addPar("weightAllocationStrategy", defaultWeightAllocationStrategy);
        return defaultRandomAllocStrategy;
    }

    public static Strategy getDefaultEqualAlloc() {
        var defaultEqualAllocStrategy = new Strategy();
        defaultEqualAllocStrategy.name = StrategyName.Equal;
        defaultEqualAllocStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultEqualAllocStrategy.addPar("recordIterState", defaultRecordIterState);
        defaultEqualAllocStrategy.addPar("weightAllocationStrategy", defaultWeightAllocationStrategy);
        return defaultEqualAllocStrategy;
    }

    public static Strategy getDefaultTwoStep() {
        var defaultTwoStepStrategy = new Strategy();
        defaultTwoStepStrategy.name = StrategyName.TwoStepComb;
        defaultTwoStepStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultTwoStepStrategy.addPar("initSize", defaultInitSize);
        defaultTwoStepStrategy.addPar("recordIterState", defaultRecordIterState);
        defaultTwoStepStrategy.addPar("weightAllocationStrategy", defaultWeightAllocationStrategy);
        return defaultTwoStepStrategy;
    }

    public static Strategy getDefaultBlinkDb() {
        var defaultBlinkDBStrategy = new Strategy();
        defaultBlinkDBStrategy.name = StrategyName.BlinkDB;
        defaultBlinkDBStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultBlinkDBStrategy.addPar("initSize", defaultInitSize);
        defaultBlinkDBStrategy.addPar("recordIterState", defaultRecordIterState);
        return defaultBlinkDBStrategy;
    }

    public static Strategy getDefaultSingleSamplerStrategy() {
        var defaultSingleSamplerStrategy = new Strategy();
        defaultSingleSamplerStrategy.name = StrategyName.SingleSampler;
        defaultSingleSamplerStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultSingleSamplerStrategy.addPar("batchType", defaultBatchType);
        defaultSingleSamplerStrategy.addPar("batchSize", defaultBatchSize);
        defaultSingleSamplerStrategy.addPar("recordIterState", defaultRecordIterState);
        return defaultSingleSamplerStrategy;
    }

    public static Strategy getDefaultOfflineOptimalStrategy() {
        // offline optimal means the real optimal sampler in all samplers.
        var defaultOfflineOptimalStrategy = new Strategy();
        defaultOfflineOptimalStrategy.name = StrategyName.OfflineOptimal;
        defaultOfflineOptimalStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultOfflineOptimalStrategy.addPar("batchType", defaultBatchType);
        defaultOfflineOptimalStrategy.addPar("batchSize", defaultBatchSize);
        defaultOfflineOptimalStrategy.addPar("recordIterState", defaultRecordIterState);
        return defaultOfflineOptimalStrategy;
    }

    public static Strategy getDefaultLcb() {
        var defaultLcbStrategy = new Strategy();
        defaultLcbStrategy.name = StrategyName.Lcb;
        defaultLcbStrategy.addPar("totalBudget", defaultTotalBudget);
        defaultLcbStrategy.addPar("initSize", defaultInitSize);
        defaultLcbStrategy.addPar("batchType", defaultBatchType);
        defaultLcbStrategy.addPar("batchSize", defaultBatchSize);
        defaultLcbStrategy.addPar("lcbWeight", 1.0);
        defaultLcbStrategy.addPar("boundType", "hoeffding");
        defaultLcbStrategy.addPar("recordIterState", defaultRecordIterState);
        defaultLcbStrategy.addPar("weightAllocationStrategy", defaultWeightAllocationStrategy);
        return defaultLcbStrategy;
    }

    public static Strategy getDefaultAdaptiveLcb() {
        var defaultAdaptiveLcbStrategy = getDefaultLcb().copy();
        defaultAdaptiveLcbStrategy.name = StrategyName.AdaptiveLcb;
        return defaultAdaptiveLcbStrategy;
    }


}
