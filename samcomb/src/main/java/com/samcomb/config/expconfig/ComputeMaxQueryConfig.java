package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;

public class ComputeMaxQueryConfig {

    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("queryAggregateType", AggregateType.MAX);
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("usePrecomputeSample", DefaultExpConfig.defaultUsePrecomputeSample);
            config.getConfig(schema).add("queryPath", DefaultExpConfig.defaultQueryPaths.get(schema));
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultSamplers.get(schema + ".types"));
        }

        return config;
    }

    public static ArrayList<Strategy> getStrategies(Config expConfig) {

        var strategies = new ArrayList<Strategy>();
        var samplerColumns = expConfig.getStringList("samplerColumns");
        var samplerTypes = expConfig.getStringList("samplerTypes");
        var samplerNames = DefaultExpConfig.getSamplerNames(samplerColumns, samplerTypes);
        var samplerArray = new ArrayList<String>();
        for (var sname : samplerNames) {
            samplerArray.add(sname);
            var availableSamplers = new ArrayList<>(samplerArray);
            var eps = DefaultStrategyConfig.getDefaultEps().copy();
            int batchSize = (int) eps.getPar("batchSize");
            eps.addPar("initSize", batchSize * samplerNames.size());
            eps.addPar("recordIterState", true);
            eps.addPar("availableSamplers", availableSamplers);
            eps.addPar("samplerNum", availableSamplers.size());
            strategies.add(eps);
        }

        return strategies;
    }
}
