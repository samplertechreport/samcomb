package com.samcomb.experiment.exp;

import com.samcomb.config.Config;
import com.samcomb.dataframe.Dataframe;
import com.samcomb.utils.Logger;
import com.samcomb.utils.Rander;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class CreateSamplerTables {

    /*
    public static HashMap<String, Estimator> initSamplers(Query query, HashMap<String, Double> varzBounds, double lcbWeight, String[] samplerNames){
        var samplers = new HashMap<String, Estimator>();

        for (String samplerName : samplerNames) {

            //  samplers.put(samplerName, new GeneralSampler(samplerName,samplerFilePath, probCol, varzBound, query, lcbWeight));
        }
        return samplers;
    }
*/


    private static String getUfProbSqlClause(String tableName, Connection conn) throws SQLException, IOException {
        var cntSql = String.format("SELECT count(*) as cnt FROM %s", tableName);
        var stat = conn.createStatement();
        Logger.log(cntSql);
        var resultSet = stat.executeQuery(cntSql);
        resultSet.next();

        assert resultSet.getMetaData().getColumnCount() == 1;
        int size = resultSet.getInt(1);
        stat.close();

        assert size > 0;
        return String.format("1.0/%d", size);
    }



    /*
    private  static String getMbProbSqlClause(String column, String tableName, Connection conn) throws SQLException, IOException {
        var minPositiveSql = String.format("SELECT MIN(ABS(%s)) FROM %s WHERE ABS(%s) > 0.0", column, tableName ,column);
        var minPositivaValue = Dataframe.getSingleValueFromSql(minPositiveSql, conn);
        assert minPositivaValue > 0;

        var normedSumSql = String.format("SELECT SUM(CASE WHEN ABS(%s) > 0 THEN ABS(%s) ELSE %f END) as sumq FROM %s", column, column, minPositivaValue, tableName);
        var sum = Dataframe.getSingleValueFromSql(normedSumSql, conn);
        Logger.log("sum=" + sum);
        assert sum > 0.0;  // (MathTool.equal(sum, 0.0)) return getUfProbSqlClause();
        return String.format("CASE WHEN ABS(%s) > 0.0 THEN ABS(%s)/%f ELSE %f/%f END", column, column, sum, minPositivaValue, sum);
    }
*/


    private static String getMbProbSqlClause(String column, String tableName, Connection conn) throws SQLException, IOException {
        var aggSql = String.format("SELECT count(*) as cnt, SUM(ABS(%s)) as sum FROM %s", column, tableName);
        var stat = conn.createStatement();
        // Logger.log(aggSql);
        var resultSet = stat.executeQuery(aggSql);
        resultSet.next();

        assert resultSet.getMetaData().getColumnCount() == 2;
        double size = resultSet.getInt("cnt");
        double sum = resultSet.getDouble("sum");
        stat.close();
        //set the minProb.
        String minProbStr = String.format("1.0/%f/%f", 10000.0, size);
        //  Logger.log("sum=" + sum);
        return String.format("(ABS(%s)/%f)*(1.0 - %s) + %s", column, sum, minProbStr, minProbStr);
    }


    /**
     * create bucket-based measure-biased sampler.
     *
     * @param column
     * @param tableName
     * @param conn
     * @return
     */
    public static String getBkProbSqlClause(String column, String tableName, Connection conn) throws IOException, SQLException {

        var aggSql = String.format("SELECT min(%s) as minv, max(%s) as maxv FROM %s", column, column, tableName);
        var row = Dataframe.fromSql(conn, aggSql).getRow(0);
        String lb = row.get("minv").toString();
        String ub = row.get("maxv").toString();
        int bucketNum = 100;

        String bucketClause = String.format("(width_bucket(%s, %s, %s, %d) + 1.0)", column, lb, ub, bucketNum); // +1.0 to avoid the case when bucket=0
        String bucketSumSql = String.format("SELECT SUM(%s) FROM %s", bucketClause, tableName);
        String bucketSumInStr = Dataframe.getSingleValueFromSql(bucketSumSql, conn, false).toString();
        Logger.logger.info("bucketSumInStr" + bucketSumInStr);
        var res = String.format("(%s)/(%s)", bucketClause, bucketSumInStr);
        Logger.logger.info(res);
        return res;
    }


    private static String getSfProbSqlClause(String column, String tableName, Connection conn) throws SQLException, IOException {
        var gSql = String.format("SELECT %s as group, COUNT(*) as cnt FROM %s GROUP BY %s", column, tableName, column);
        var groupCntDf = Dataframe.fromSql(conn, gSql);

        int totalGroups = groupCntDf.getRowNum();

        var probSql = "CASE";
        for (int rid = 0; rid < totalGroups; rid++) {
            var row = groupCntDf.getRow(rid);
            var groupName = row.get("group");
            var groupSize = (long) row.get("cnt");
            // write expression rather than compute the weight is to avoid the precision loss.
            var weight = String.format("1.0/%d.0/%d.0", totalGroups, groupSize);
            var sql = String.format(" WHEN %s = '%s' THEN %s", column, groupName, weight);
            probSql += sql;
        }
        probSql += " END";

        return probSql;
    }


    public static String getProbSqlClause(String sampleType, String samplerColumn, String tableName, Connection conn) throws IOException, SQLException {
        switch (sampleType) {
            case "uf":
                return getUfProbSqlClause(tableName, conn);
            case "mb":
                return getMbProbSqlClause(samplerColumn, tableName, conn);
            case "sf":
                return getSfProbSqlClause(samplerColumn, tableName, conn);
            case "bk":
                return getBkProbSqlClause(samplerColumn, tableName, conn);
            default:
                assert false : "unknown sampler type " + sampleType;
                return "";
        }
    }


    public static void createSamplerProbTables(Connection conn, String orgtable, String populationTable, List<String> samplerColumns, List<String> samplerTypes) throws SQLException, IOException {
        // var rowIdCol = config.getString("rowIdColumnName");


        //  Logger.log("schemaName:" + schemaName);
        int nsampler = samplerColumns.size();

        StringJoiner sj = new StringJoiner(",");
        for (int i = 0; i < nsampler; i++) {
            //  var stat = conn.createStatement();
            var stype = samplerTypes.get(i);
            var sname = samplerColumns.get(i);
            var samplerName = stype + "_" + sname;
            Logger.logger.info("start creating sample prob. table for sampler:" + samplerName);
            var probSql = getProbSqlClause(samplerTypes.get(i), samplerColumns.get(i), orgtable, conn) + " as " + samplerName;
            sj.add(probSql);
            /*
            var probTableName = String.format(" %s.%s_%s_prob", schemaName, stype, sname);
            stat.execute(String.format("DROP TABLE IF EXISTS %s;", probTableName));
            var sql = String.format("CREATE TABLE %s AS SELECT rowid, %s as prob FROM %s;", probTableName, probSql, orgtableName);
            stat.execute(sql);
            stat.close();
            Logger.log("finish creating sample prob. table for sampler:"+stype+"_"+sname + ", tableName =" + probTableName);
            */
        }

        //  var sql = sj.toString();

        String dropSql = "DROP TABLE IF EXISTS " + populationTable;
        String createSql = String.format("CREATE TABLE %s AS SELECT *, %s FROM %s;", populationTable, sj.toString(), orgtable);
        String insertRowIdSql = String.format("ALTER TABLE %s ADD COLUMN rowid SERIAL PRIMARY KEY;", populationTable);

        var stat = conn.createStatement();
        Logger.logger.info(dropSql);
        stat.execute(dropSql);
        Logger.logger.info(createSql);
        stat.execute(createSql);
        Logger.logger.info(insertRowIdSql);
        stat.execute(insertRowIdSql);
        stat.close();
    }

    public static void createSamplerDataTablesWithReplacement(Connection conn,
                                                              int sampleSize,
                                                              String schemaName,
                                                              String populationTable,
                                                              List<String> samplerColumns,
                                                              List<String> samplerTypes,
                                                              int run_id) throws SQLException, IOException {
        // var rowIdCol = config.getString("rowIdColumnName");
        // var samplerColumns = config.getStringList("samplerColumns");
        // var samplerTypes = config.getStringList("samplerTypes");
        // assert samplerColumns.size() == samplerTypes.size();
        var nsampler = samplerColumns.size();

        //  var schemaName =config.getString("schemaName");
        //  var populationTable = config.getString("populationTable");

        var randWeights = new ArrayList<Double>();
        for (int i = 0; i < sampleSize; i++) {
            randWeights.add(Rander.rand.nextDouble());
        }
        Collections.sort(randWeights);

        for (int si = 0; si < nsampler; si++) {
            var stat = conn.createStatement();
            stat.setFetchSize(1000);
            var stype = samplerTypes.get(si);
            var sname = samplerColumns.get(si);
            Logger.log("building sample data table for sampler:" + stype + "_" + sname);

            // var probTableName = String.format(" %s.%s_%s_prob", schemaName, stype, sname);
            var probCol = stype + "_" + sname;
            var tmpTableName = String.format("tmp_%s_%s_%s_%d", schemaName, stype, sname, run_id);

            var insertStat = conn
                    .prepareStatement(String.format("INSERT INTO %s(rowid, prob) VALUES (?, ?)", tmpTableName));

            stat.execute(String.format("DROP TABLE IF EXISTS %s;", tmpTableName));
            stat.execute(String.format("CREATE TEMP TABLE %s(rowid integer, prob numeric);", tmpTableName));
            var rs = stat.executeQuery(String.format("SELECT rowid, %s FROM %s;", probCol, populationTable));

            double presum = 0.0;
            int total_copies = 0;
            while (rs.next()) {
                var prob = rs.getDouble(probCol);
                var rowId = rs.getInt("rowid");
                int ind1 = Collections.binarySearch(randWeights, presum);
                if (ind1 < 0)
                    ind1 = -ind1 - 1;
                int ind2 = Collections.binarySearch(randWeights, presum + prob);
                if (ind2 < 0)
                    ind2 = -ind2 - 1 - 1;
                else
                    ind2 = ind2 - 1;
                int ncopies = Math.max(0, ind2 - ind1 + 1);
                for (int ci = 0; ci < ncopies; ci++) {
                    insertStat.setInt(1, rowId);
                    insertStat.setDouble(2, prob);
                    insertStat.addBatch();
                }
                total_copies += ncopies;
                presum += prob;
            }

            stat.close();

            insertStat.executeBatch();
            insertStat.close();
            Logger.logger.info("inserted " + total_copies + " rows into tmp table, start joining it with population");

            var dataProbTableName = String.format(" %s.%s_%s_sample_run%d", schemaName, stype, sname, run_id);
            var joinStat = conn.createStatement();
            joinStat.execute(String.format("DROP TABLE IF EXISTS %s", dataProbTableName));
            joinStat.execute(String.format("SELECT setseed(%f)", Rander.rand.nextDouble())); // to seed the random used by join
            joinStat.execute(String.format(
                    "CREATE TABLE %s AS SELECT RANDOM() as randCol, * FROM %s JOIN %s USING (rowid) ORDER BY randCol",
                    dataProbTableName, populationTable, tmpTableName));
            joinStat.execute(String.format("DROP TABLE IF EXISTS %s;", tmpTableName));
            joinStat.close();
            Logger.logger.info("build sampler table done, generated table:" + dataProbTableName);
        }
    }


    public static void runOnSingleSchema(Config config, String schemaName) throws SQLException, IOException {
        String jdbc = config.getString("jdbc");
        Connection conn = DriverManager.getConnection(jdbc);

        String orgTable = config.getString("orgTable");
        String populationTable = config.getString("populationTable");
        var samplerColumns = config.getStringList("samplerColumns");
        var samplerTypes = config.getStringList("samplerTypes");
        double sampleSize = config.getInt("preCreateSampleSizePerSampler");
        int nruns = config.getInt("nruns");
        if (sampleSize <= 1.0000001) {
            var tableRows = (long) Dataframe.getSingleValueFromSql(String.format("SELECT COUNT(*) AS cnt FROM %s", orgTable), conn, true);
            sampleSize *= tableRows;
        }
        if (samplerColumns.size() != samplerTypes.size()) {
            throw new IllegalStateException("sampeler column size does not equal to sampler type size");
        }
        createSamplerProbTables(conn, orgTable, populationTable, samplerColumns, samplerTypes);
        for (int run_id = 0; run_id < nruns; run_id++) {
            // for each run create a random sample for each sampler with given sample size.
            createSamplerDataTablesWithReplacement(conn, (int) sampleSize, schemaName, populationTable, samplerColumns,
                    samplerTypes, run_id);
        }
        conn.close();
    }

    public static void run(Config config) throws Exception {

        var schemaNames = config.getStringList("schemaNames");

        for (var schemaName : schemaNames) {
            var subConfig = config.getConfig(schemaName);
            runOnSingleSchema(subConfig, schemaName);
        }
    }

}
