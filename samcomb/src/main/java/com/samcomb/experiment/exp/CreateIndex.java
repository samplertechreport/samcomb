package com.samcomb.experiment.exp;

import com.samcomb.config.Config;
import com.samcomb.config.ExpConfigManager;
import com.samcomb.utils.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class CreateIndex {

    public static void runOnSingleSchema(Config config, String schemaName) throws SQLException {
        String jdbc = config.getString("jdbc");
        Connection conn = DriverManager.getConnection(jdbc);

        String populationTable = config.getString("populationTable");
        List<String> indexColumns = config.getStringList("popuIndexColumns");
        for (var col : indexColumns) {
            String sql = String.format("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s(%s);", schemaName, col,
                    populationTable, col);
            Logger.logger.info(sql);
            var stat = conn.createStatement();
            stat.execute(sql);
            stat.close();
        }

        var samplerColumns = config.getStringList("samplerColumns");
        var sampelerTypes = config.getStringList("samplerTypes");
        int nruns = config.getInt("nruns");
        for (int i = 0; i < samplerColumns.size(); i++) {
            try {
                for (int run_id = 0; run_id < nruns; run_id++) {
                    var samplerTableName = sampelerTypes.get(i) + "_" + samplerColumns.get(i) + "_sample_run" + run_id;
                    String sql = String.format("CREATE INDEX IF NOT EXISTS idx_%s_%s_randcol ON %s.%s( randcol );",
                            schemaName, samplerTableName, schemaName, samplerTableName);
                    Logger.logger.info(sql);
                    var stat = conn.createStatement();
                    stat.execute(sql);
                    stat.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

        }

        conn.close();
    }

    public static void run(ExpName expname) throws SQLException {
        var config = ExpConfigManager.getExpConfig(expname);
        var schemaNames = config.getStringList("schemaNames");
        for (var schemaName : schemaNames) {
            var subConfig = config.getConfig(schemaName);
            runOnSingleSchema(subConfig, schemaName);
        }
    }
}
