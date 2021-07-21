package com.samcomb.config;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.expconfig.*;
import com.samcomb.experiment.exp.ExpName;

import java.util.ArrayList;

public class ExpConfigManager {


    public static ArrayList<Strategy> getStrategies(ExpName expName, Config expConfig) {
        switch (expName) {
            case compareOfflineOptimalAndOnlineOptimalSampler: {
                return CompareOfflineOptimalAndOnlineOptimalConfig.getStrategies(expConfig);
            }

            case compareTwoStepCombAndBlinkDb: {
                return CompareTwoStepCombAndBlinkDbConfig.getStrategies(expConfig);
            }

            case computeMedianQuery: {
                return ComputeMedianQueryConfig.getStrategies(expConfig);
            }

            case computeMaxQuery: {
                return ComputeMaxQueryConfig.getStrategies(expConfig);
            }
            case varySampelerNum: {
                return VarySamplerNumConfig.getStrategies(expConfig);
            }

            case varyC: {
                return VaryCConfig.getStrategies(expConfig);
            }

            case compareLcbEps: {
                return CompareLcbEpsConfig.getStrategies(expConfig);
            }

            case combineVsNotcombine: {
                return CombineVsNotCombineConfig.getStrategies(expConfig);
            }

            case selectivityEstimation: {
                return SelectivityEstimationConfig.getStrategies(expConfig);
            }

            case varySampleSize: {
                return VarySampleSizeConfig.getStrategies(expConfig);
            }

            case varyDataSize: {
                return VaryDataSizeConfig.getStrategies(expConfig);
            }
            case rankSingleSampler: {
                return RankSingleSamplerConfig.getStrategies(expConfig);
            }

            default:
                throw new IllegalArgumentException("unhandled expName:" + expName);
        }
    }

    public static Config getExpConfig(ExpName expName) {
        switch (expName) {

            case buildSamplers: {
                return CreateSamplerTablesConfig.getExpConfig();
            }

            case generateQuery: {
                return GenerateQueryConfig.getExpConfig();
            }

            case compareOfflineOptimalAndOnlineOptimalSampler: {
                return CompareOfflineOptimalAndOnlineOptimalConfig.getExpConfig();
            }

            case createIndex: {
                return CreateIndexConfig.getExpConfig();
            }

            case compareTwoStepCombAndBlinkDb: {
                return CompareTwoStepCombAndBlinkDbConfig.getExpConfig();
            }

            case computeMedianQuery: {
                return ComputeMedianQueryConfig.getExpConfig();
            }

            case computeMaxQuery: {
                return ComputeMaxQueryConfig.getExpConfig();
            }

            case varySampelerNum: {
                return VarySamplerNumConfig.getExpConfig();
            }

            case varyC: {
                return VaryCConfig.getExpConfig();
            }

            case compareLcbEps: {
                return CompareLcbEpsConfig.getExpConfig();
            }

            case combineVsNotcombine: {
                return CombineVsNotCombineConfig.getExpConfig();
            }

            case selectivityEstimation: {
                return SelectivityEstimationConfig.getExpConfig();
            }

            case varySampleSize: {
                return VarySampleSizeConfig.getExpConfig();
            }

            case varyDataSize: {
                return VaryDataSizeConfig.getExpConfig();
            }

            case rankSingleSampler: {
                return RankSingleSamplerConfig.getExpConfig();
            }

            default: {
                throw new IllegalStateException("unprocessed config of expname=" + expName);
            }


        }
    }
}
