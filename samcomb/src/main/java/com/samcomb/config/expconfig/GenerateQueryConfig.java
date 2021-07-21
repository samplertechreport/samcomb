package com.samcomb.config.expconfig;

import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.Environment;

import java.util.Arrays;

public class GenerateQueryConfig {

    public static Config getExpConfig() {

        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2", "loan"));
        // config.add("schemaNames", Arrays.asList("skew_s05_z2"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }

        for (var schemaName : config.getStringList("schemaNames")) {

            config.getConfig(schemaName).add("jdbc", DefaultExpConfig.defaultJdbc);
            // config.getConfig(schemaName).add("queryNum", DefaultExpConfig.getEnvironment().equals(Environment.Remote)? 50:2);
            config.getConfig(schemaName).add("queryNum", DefaultExpConfig.getEnvironment().equals(Environment.Remote) ? 50 : 2);
            config.getConfig(schemaName).add("minSelectively", 0.001);
            config.getConfig(schemaName).add("maxSelectively", 0.01);
//            config.getConfig(schemaName).add("minSelectively", 0.005);
//            config.getConfig(schemaName).add("maxSelectively", 0.05);
            config.getConfig(schemaName).add("minSamplerDiffs", Arrays.asList(1.0, 1.5));
            config.getConfig(schemaName).add("maxSamplerDiffs", Arrays.asList(1.5, 100.0));
            config.getConfig(schemaName).add("aggregateFunction", "sum");
            config.getConfig(schemaName).add("sampleRate", 1.0);
            config.getConfig(schemaName).add("tableName", schemaName + ".population");
            config.getConfig(schemaName).add("savePath", DefaultExpConfig.defaultQueryPaths.get(schemaName));
            config.getConfig(schemaName).add("additionalPredicates", Arrays.asList("", ""));
            config.getConfig(schemaName).add("samplerColumns", DefaultExpConfig.defaultSamplers.get(schemaName + ".columns"));
            config.getConfig(schemaName).add("samplerTypes", DefaultExpConfig.defaultSamplers.get(schemaName + ".types"));
        }

        // config.getConfig("skew_s1_z2").add("predicateColumns", Arrays.asList("l_shipdate", "l_extendedprice", "l_orderkey", "l_discount", "l_returnflag"));
        if (config.contains("skew_s1_z2")) {
            config.getConfig("skew_s1_z2").add("predicateColumns", Arrays.asList("l_quantity", "l_extendedprice", "l_orderkey", "l_discount", "l_returnflag"));
            config.getConfig("skew_s1_z2").add("aggregateColumns", Arrays.asList("l_quantity", "l_extendedprice", "l_discount", "l_tax"));
        }

        if (config.contains("skew_s05_z2")) {
            config.getConfig("skew_s05_z2").add("predicateColumns", Arrays.asList("l_quantity", "l_extendedprice", "l_orderkey", "l_discount", "l_returnflag"));
            config.getConfig("skew_s05_z2").add("aggregateColumns", Arrays.asList("l_quantity", "l_extendedprice", "l_discount", "l_tax"));
        }

        if (config.contains("loan")) {
            config.getConfig("loan").add("predicateColumns", Arrays.asList("borrower_rate", "installment", "principal_balance", "grade", "loan_status_description"));
            config.getConfig("loan").add("aggregateColumns", Arrays.asList("amount_borrowed", "borrower_rate", "installment"));
        }

        return config;
    }
}
