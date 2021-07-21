package com.samcomb.experiment.exp;

import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.utils.Logger;
import com.samcomb.utils.Rander;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class VaryDataExpPreprocessor {

    public static void createOrgTables(Config config, List<Double> dataratios) throws SQLException {
        var jdbcUrl = config.getString("jdbc");
        Connection conn = DriverManager.getConnection(jdbcUrl);
        var stat = conn.createStatement();
        var orgSchema = config.getString("orgSchema"); // "skew_s1_z2";
        var orgTable = orgSchema + ".orgtable";
        for (double r : dataratios) {
            String suffix = "_" + (int) (r * 100) + "per";
            var currentSchema = orgSchema + suffix;
            var randseed = String.format("SELECT setseed(%f)", Rander.rand.nextDouble());
            stat.execute(randseed);

            var createSchema = "CREATE SCHEMA IF NOT EXISTS " + currentSchema;
            Logger.logger.info(createSchema);
            stat.execute(createSchema);

            var currentOrgTable = currentSchema + ".orgtable";
            var dropTable = String.format("DROP TABLE IF EXISTS %s", currentOrgTable);
            stat.execute(dropTable);
            Logger.logger.info("create table:" + currentOrgTable + ", ratio=" + r);
            String createSql = String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE RANDOM() < %f;",
                    currentOrgTable, orgTable, r);
            stat.execute(createSql);
        }
        stat.close();
        conn.close();
    }

    public static Config getCreateSamplerConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2_10per", "skew_s1_z2_20per", "skew_s1_z2_30per",
                "skew_s1_z2_40per", "skew_s1_z2_50per", "skew_s1_z2_60per"));
        // config.add("schemaNames", Arrays.asList("skew_s1_z2", "loan"));
        // config.add("schemaNames", Arrays.asList("skew_s1_z2"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("preCreateSampleSizePerSampler", 300000);
            // nruns is the number of samples generated for each sampler, id from 0 to nruns
            // with different random seed.
            config.getConfig(schema).add("nruns", DefaultExpConfig.nruns);
            config.getConfig(schema).add("orgTable", schema + ".orgtable");
            config.getConfig(schema).add("populationTable", schema + ".population");
            if (schema.startsWith("skew")) {
                config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultSamplers.get("skew.columns"));
                config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultSamplers.get("skew.types"));
            } else {
                config.getConfig(schema).add("samplerColumns",
                        DefaultExpConfig.defaultSamplers.get(schema + ".columns"));
                config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultSamplers.get(schema + ".types"));
            }
        }
        return config;
    }

    public static void run(Config config) throws Exception {
        var dataRatios = config.getDoubleList("dataRatios");
        createOrgTables(config, dataRatios);
        CreateSamplerTables.run(getCreateSamplerConfig());
    }
}
