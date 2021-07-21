package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;

// rank single sampler performance based on all queries. 
// this is used for deciding the sampler order in varying sampler number exp
public class RankSingleSamplerConfig {

    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("queryAggregateType", AggregateType.SUM);
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("usePrecomputeSample", DefaultExpConfig.defaultUsePrecomputeSample);
            config.getConfig(schema).add("queryPath", DefaultExpConfig.defaultQueryPaths.get(schema));
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultAllSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultAllSamplers.get(schema + ".types"));
        }

        return config;
    }

    public static ArrayList<Strategy> getStrategies(Config expConfig) {

        var strategies = new ArrayList<Strategy>();
        var samplerColumns = expConfig.getStringList("samplerColumns");
        var samplerTypes = expConfig.getStringList("samplerTypes");
        int nsampler = samplerColumns.size();
        if (nsampler != samplerTypes.size())
            throw new IllegalStateException("sampler column size != sampler type size!");
        for (int i = 0; i < nsampler; i++) {
            var samplerName = samplerTypes.get(i) + "_" + samplerColumns.get(i);
            var singleStrategy = DefaultStrategyConfig.getDefaultSingleSamplerStrategy().copy();
            singleStrategy.addPar("singleSamplerName", samplerName);
            strategies.add(singleStrategy);
        }
        return strategies;
    }
}
