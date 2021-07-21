package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.dataframe.MathTool;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;

public class VaryCConfig {
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

    public static ArrayList<Strategy> getStrategies(Config expConfig) {

        var strategies = new ArrayList<Strategy>();
        var cList = MathTool.arange(0, 300, 10);
        for (double c : cList) {
            var eps = DefaultStrategyConfig.getDefaultEps().copy();
            eps.addPar("decayConstant", c);
            strategies.add(eps);
        }

        return strategies;
    }
}
