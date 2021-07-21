package com.samcomb.config.expconfig;

import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;

import java.util.Arrays;

public class VaryDataPrecomputeTableConfig {

    public static Config getExpConfig() {
        var config = new Config();
        config.add("jdbc", DefaultExpConfig.defaultJdbc);
        config.add("orgSchema", "skew_s1_z2");
        config.add("dataRatios", Arrays.asList(0.1, 0.2, 0.3, 0.4, 0.5, 0.6));
        return config;
    }
}
