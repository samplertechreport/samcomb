package com.samcomb.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DefaultExpConfig {

    public static final String defaultJdbc = getJdbc();

    public static final int nruns = 5;

    public static final HashMap<String, List<String>> defaultAllSamplers = new HashMap<String, List<String>>() {{
        put("skew_s1_z2.columns", DatasetInfos.tpchAllColumns);
        put("skew_s1_z2.types", DatasetInfos.tpchAllSamplerTypes);
        put("skew_s05_z2.columns", DatasetInfos.tpchAllColumns);
        put("skew_s05_z2.types", DatasetInfos.tpchAllSamplerTypes);
        put("loan.columns", DatasetInfos.loanAllColumns);
        put("loan.types", DatasetInfos.loanAllTypes);
    }};


    public static final HashMap<String, List<String>> defaultSamplers = new HashMap<String, List<String>>() {{
        // put("skew_s1_z2.columns", Arrays.asList("None", "l_shipdate", "l_extendedprice"));
        put("skew.columns", Arrays.asList("None", "l_returnflag", "l_extendedprice"));
        put("skew.types", Arrays.asList("uf", "sf", "bk"));
        put("skew_s1_z2.columns", Arrays.asList("None", "l_returnflag", "l_extendedprice"));
        put("skew_s1_z2.types", Arrays.asList("uf", "sf", "bk"));
        put("skew_s05_z2.columns", Arrays.asList("None", "l_returnflag", "l_extendedprice"));
        put("skew_s05_z2.types", Arrays.asList("uf", "sf", "bk"));
        put("loan.columns", Arrays.asList("None", "grade", "principal_balance"));
        put("loan.types", Arrays.asList("uf", "sf", "bk"));
    }};


    public static final String defaultOutputFolder = getOutputFolder();
    public static final String defaultQueryFolder = defaultOutputFolder + "/queries";
    public static final HashMap<String, String> defaultQueryPaths = new HashMap<String, String>() {{
        put("skew_s1_z2", defaultQueryFolder + "/tpch_query.csv");
        put("loan", defaultQueryFolder + "/loan_query.csv");
    }};


    public static final boolean defaultUsePrecomputeSample = true;


    public static List<String> getSamplerNames(List<String> samplerColumns, List<String> samplerTypes) {
        if (samplerColumns.size() != samplerTypes.size())
            throw new IllegalArgumentException("sampler column size should be equal to sampler type size");
        var samplerNames = new ArrayList<String>();
        for (int i = 0; i < samplerColumns.size(); i++) {
            samplerNames.add(samplerTypes.get(i) + "_" + samplerColumns.get(i));
        }
        return samplerNames;
    }


    public static Environment getEnvironment() {
        return Environment.Local;
    }

    public static String getJdbc() {
        var env = getEnvironment();
        if (env.equals(Environment.Local)) {
            return "jdbc:postgresql://localhost/samcomb?user=USER&password=PASSWORD";
        }
        if (env.equals(Environment.Remote)) {
            return "jdbc:postgresql://REMOTE_IP:5432/samcomb?user=USER&password=PASSWORD";
        }
        throw new IllegalStateException("unprocessed environment:" + env);
    }

    public static String getOutputFolder() {

        var env = getEnvironment();
        if (env.equals(Environment.Local)) {
            return "output";
        }
        if (env.equals(Environment.Remote)) {
            return "remote_output";
        }
        throw new IllegalStateException("unprocessed environment:" + env);
    }
}
