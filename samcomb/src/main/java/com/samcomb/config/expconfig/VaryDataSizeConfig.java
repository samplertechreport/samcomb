package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;

public class VaryDataSizeConfig {

    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList(
                "skew_s1_z2_10per",
                "skew_s1_z2_20per",
                "skew_s1_z2_30per",
                "skew_s1_z2_40per",
                "skew_s1_z2_50per",
                "skew_s1_z2_60per"));

        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("queryAggregateType", AggregateType.SUM);
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("usePrecomputeSample", DefaultExpConfig.defaultUsePrecomputeSample);
            config.getConfig(schema).add("queryPath", DefaultExpConfig.defaultQueryPaths.get("skew_s1_z2"));
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultSamplers.get("skew_s1_z2" + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultSamplers.get("skew_s1_z2" + ".types"));
        }
        return config;
    }

    public static ArrayList<Strategy> getStrategies(Config expConfig) {
        var stratigies = new ArrayList<Strategy>();
        var columns = expConfig.getStringList("samplerColumns");
        var types = expConfig.getStringList("samplerTypes");
        var samplerNames = DefaultExpConfig.getSamplerNames(columns, types);
        var samplerArray = new ArrayList<String>();
        for (var sname : samplerNames) {
            samplerArray.add(sname);
            var availableSamplers = new ArrayList<>(samplerArray);

            var adaptive = DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
            int batchSize = (int) adaptive.getPar("batchSize");
            adaptive.addPar("initSize", batchSize * samplerNames.size());
            adaptive.addPar("availableSamplers", availableSamplers);
            adaptive.addPar("samplerNum", availableSamplers.size());
            stratigies.add(adaptive);
        }
        return stratigies;
    }


}
