package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.allocation.StrategyName;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.types.AggregateType;

import java.util.ArrayList;
import java.util.Arrays;

public class CompareOfflineOptimalAndOnlineOptimalConfig {

    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2", "loan"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("usePrecomputeSample", DefaultExpConfig.defaultUsePrecomputeSample);
            config.getConfig(schema).add("queryPath", DefaultExpConfig.defaultQueryPaths.get(schema));
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("queryAggregateType", AggregateType.SUM);
            config.getConfig(schema).add("onlineSamplerColumns", DefaultExpConfig.defaultSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("onlineSamplerTypes", DefaultExpConfig.defaultSamplers.get(schema + ".types"));
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultAllSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultAllSamplers.get(schema + ".types"));
        }

        return config;
    }


    public static ArrayList<Strategy> getStrategies(Config expConfig) {

        var stratigies = new ArrayList<Strategy>();

        var onlineSamplers = DefaultExpConfig.getSamplerNames(
                expConfig.getStringList("onlineSamplerColumns"),
                expConfig.getStringList("onlineSamplerTypes")
        );


        var offlineOptimal = new Strategy();
        offlineOptimal.name = StrategyName.OfflineOptimal;
        offlineOptimal.addPar("totalBudget", DefaultStrategyConfig.defaultTotalBudget);
        offlineOptimal.addPar("batchType", DefaultStrategyConfig.defaultBatchType);
        offlineOptimal.addPar("batchSize", DefaultStrategyConfig.defaultBatchSize);
        offlineOptimal.addPar("recordIterState", DefaultStrategyConfig.defaultRecordIterState);
        stratigies.add(offlineOptimal);


        var onlineOptimal = new Strategy();
        onlineOptimal.name = StrategyName.OnlineOptimal;
        onlineOptimal.addPar("totalBudget", DefaultStrategyConfig.defaultTotalBudget);
        onlineOptimal.addPar("batchType", DefaultStrategyConfig.defaultBatchType);
        onlineOptimal.addPar("batchSize", DefaultStrategyConfig.defaultBatchSize);
        onlineOptimal.addPar("recordIterState", DefaultStrategyConfig.defaultRecordIterState);
        onlineOptimal.addPar("onlineSamplers", onlineSamplers);
        stratigies.add(onlineOptimal);

        return stratigies;
    }

}
