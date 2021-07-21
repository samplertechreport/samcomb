package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.dataframe.MathTool;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class VarySampleSizeConfig {

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

        //0.5%-10%
        config.getConfig("skew_s1_z2").add("sampleSizes", MathTool.arange(30000, 600001, 30000));

        config.getConfig("loan").add("sampleSizes", MathTool.arange(3000, 300001, 3000));
        config.getConfig("skew_s1_z2").add("maxBudget", 600000);
        config.getConfig("loan").add("maxBudget", 300000);

        return config;
    }

    public static ArrayList<Strategy> getStrategies(Config expConfig) {

        var stratigies = new ArrayList<Strategy>();
        var samplerNames = DefaultExpConfig.getSamplerNames(expConfig.getStringList("samplerColumns"), expConfig.getStringList("samplerTypes"));
        var maxBudget = expConfig.getInt("maxBudget");
        var budgetList = (List<Integer>) expConfig.getIntegerList("sampleSizes");

        var samplerArray = new ArrayList<String>();
        for (var sname : samplerNames) {
            samplerArray.add(sname);
            var availableSamplers = new ArrayList<>(samplerArray);
            var adaptive = DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
            int batchSize = (int) adaptive.getPar("batchSize");
            adaptive.addPar("totalBudget", maxBudget);
            adaptive.addPar("initSize", batchSize * samplerNames.size());
            adaptive.addPar("recordIterState", true);
            adaptive.addPar("recordIterState.budgetList", new HashSet<Integer>(budgetList));
            adaptive.addPar("availableSamplers", availableSamplers);
            adaptive.addPar("samplerNum", availableSamplers.size());
            stratigies.add(adaptive);
        }

        return stratigies;
    }

}
