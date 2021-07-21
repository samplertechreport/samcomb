package com.samcomb.config.expconfig;

import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;

import java.util.Arrays;

public class CreateIndexConfig {


    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2", "loan"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("nruns", DefaultExpConfig.nruns);
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultAllSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultAllSamplers.get(schema + ".types"));
            // will create randcol for each sampler.
        }
        config.getConfig("skew_s1_z2").add("popuIndexColumns", Arrays.asList("l_quantity", "l_shipdate", "l_extendedprice", "l_orderkey", "l_discount", "l_returnflag"));
        // config.getConfig("skew_s05_z2").add("popuIndexColumns", Arrays.asList("l_quantity", "l_shipdate", "l_extendedprice", "l_orderkey", "l_discount", "l_returnflag"));
        config.getConfig("loan").add("popuIndexColumns", Arrays.asList("borrower_rate", "installment", "principal_balance", "grade", "loan_status_description"));

        return config;
    }


}
