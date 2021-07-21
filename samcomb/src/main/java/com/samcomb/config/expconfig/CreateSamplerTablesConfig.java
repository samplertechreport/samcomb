package com.samcomb.config.expconfig;

import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;

import java.util.Arrays;

public class CreateSamplerTablesConfig {

    public static Config getExpConfig() {
        var config = new Config();
        // config.add("schemaNames", Arrays.asList(
        //         "skew_s1_z2_10per",
        //         "skew_s1_z2_20per",
        //         "skew_s1_z2_30per",
        //         "skew_s1_z2_40per",
        //         "skew_s1_z2_50per",
        //         "skew_s1_z2_60per"));
        config.add("schemaNames", Arrays.asList("skew_s1_z2", "loan"));
        // config.add("schemaNames", Arrays.asList("skew_s1_z2"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("preCreateSampleSizePerSampler", 300000);
            // nruns is the number of samples generated for each sampler, id from 0 to nruns with different random seed. 
            config.getConfig(schema).add("nruns", DefaultExpConfig.nruns);
            config.getConfig(schema).add("orgTable", schema + ".orgtable");
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultAllSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultAllSamplers.get(schema + ".types"));

        }
        return config;
    }
}
