package com.samcomb.experiment.exp;

import com.samcomb.dataframe.Dataframe;
import com.samcomb.dataframe.MathTool;
import com.samcomb.sampler.CacheFetcher;
import com.samcomb.sampler.Estimator;
import com.samcomb.types.Query;
import com.samcomb.utils.Logger;
import com.samcomb.utils.Rander;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;

public class SamplerBuilder {

    public static Estimator createEstimatorUseCachePopu(String dataPredProbSql, Connection conn, long randseed, String estimatorName) throws IOException, SQLException {
        var df = Dataframe.fromSql(conn, dataPredProbSql);
        //  df.show();
        var data = df.get("data").parseToDoubleSeries();
        var predindicator = df.get("predindicator").parseToIntegerSeries();
        var orgProb = df.get("prob").parseToDoubleSeries();
        var probSum = MathTool.sum(orgProb);
        var normProb = MathTool.divide(orgProb, probSum);
        return new Estimator(data, predindicator, normProb, randseed, estimatorName);
    }

    public static LinkedHashMap<String, Estimator> createEstimatiorListUseCatchPopu(List<String> samplerColumnNames, List<String> samplerTypes, Query query, Connection conn) throws SQLException, IOException {

        Logger.logger.info("----------building estimator using cached population--------------");
        assert samplerColumnNames.size() == samplerTypes.size() && samplerColumnNames.size() > 0;
        var estimatorList = new LinkedHashMap<String, Estimator>();
        for (int i = 0; i < samplerColumnNames.size(); i++) {
            var col = samplerColumnNames.get(i);
            var sampType = samplerTypes.get(i);
            var randseed = Rander.rand.nextLong();

            var probSql = CreateSamplerTables.getProbSqlClause(sampType, col, query.tableName, conn);

            var aggColumnClause = query.aggFunction.toLowerCase().equals("count") ? "1" : query.aggColumn;
            var dataPredProbSql = String.format("SELECT %s as data, CASE WHEN %s THEN 1 ELSE 0 END as predindicator, %s as prob FROM %s", aggColumnClause, query.predSql, probSql, query.tableName);
            String estimatorName = sampType + "_" + col;
            var estimator = createEstimatorUseCachePopu(dataPredProbSql, conn, randseed, sampType + "_" + col);
            estimatorList.put(estimatorName, estimator);
        }

        Logger.logger.info("----------end building estimator--------------");
        return estimatorList;
    }


    public static LinkedHashMap<String, Estimator> createEstimatiorListUsePrecomputeSample(int runId, List<String> samplerColumnNames, List<String> samplerTypes, Query query, String jdbcUrl, String schemaName, boolean loadDataInMemory, int maxBudget) throws SQLException, IOException {

        Logger.logger.info("----------building estimator--------------");
        assert samplerColumnNames.size() == samplerTypes.size() && samplerColumnNames.size() > 0;
        var estimatorList = new LinkedHashMap<String, Estimator>();
        for (int i = 0; i < samplerColumnNames.size(); i++) {
            var col = samplerColumnNames.get(i);
            var sampType = samplerTypes.get(i);
            var samplerName = sampType + "_" + col;
            var dataFetcher = (loadDataInMemory) ? new CacheFetcher(runId, jdbcUrl, samplerName, query, schemaName, maxBudget) : new CacheFetcher();
            var estimator = new Estimator(jdbcUrl, samplerName, query, schemaName, dataFetcher);
            estimatorList.put(samplerName, estimator);
        }

        Logger.logger.info("----------end building estimator--------------");
        return estimatorList;
    }






    /*
    public static Estimator createEstimator(HashMap<String, Double> idToData,
                                            HashMap<String, Double> idToProb,
                                            String idColumnName,
                                            Query query,
                                            Connection conn,
                                            long randseed,
                                            String estimatorName) throws IOException, SQLException {
        var selectIdAndPredSql = String.format("SELECT %s as uid, CASE WHEN %s THEN 1 ELSE 0 END as predindicator FROM %s", idColumnName,  query.predSql, query.tableName);
        var df = Dataframe.fromSql(conn, selectIdAndPredSql);
        int rowNum = df.getRowNum();
        var data = new ArrayList<Double>();
        var orgProb = new ArrayList<Double>();
        var predindicator = new ArrayList<Integer>();
        for (int i = 0; i<rowNum; i++)
        {
            var row = df.getRow(i);
            var uid = row.get("uid").toString();
            var predInd = (int) row.get("predindicator");
            data.add(idToData.get(uid));
            orgProb.add(idToProb.get(uid));
            predindicator.add(predInd);
        }
        var probSum = MathTool.sum(orgProb);
        var normProb = MathTool.divide(orgProb, probSum);
        return new Estimator(data, predindicator, normProb ,randseed, estimatorName);
    }


    public static HashMap<String, Double> computeIdToData(String idColumnName, Query query, Connection conn) throws IOException, SQLException {
        var aggColumnClause = query.aggFunction.toLowerCase().equals("count")? "1": query.aggColumn;
        var idDataSql = String.format("SELECT %s as uid, %s as data FROM %s", idColumnName,aggColumnClause, query.tableName);
        var df = Dataframe.fromSql(conn, idDataSql);
        int rowNum = df.getRowNum();
        var idToData = new HashMap<String, Double>();
        for (int i=0; i< rowNum; i++)
        {
            var row = df.getRow(i);
            var uid = row.get("uid").toString();
            var data = (double) row.get("data");
            idToData.put(uid, data);
        }
        return idToData;
    }



    public static HashMap<String, Double> computeIdToProb(String idColumnName, String probSql, String tableName, Connection conn) throws IOException, SQLException {
        var idProbSql = String.format("SELECT %s as uid, %s as prob FROM %s", idColumnName, probSql ,tableName);
        var df = Dataframe.fromSql(conn, idProbSql);
        int rowNum = df.getRowNum();
        var idToProb = new HashMap<String, Double>();
        for (int i=0; i< rowNum; i++)
        {
            var row = df.getRow(i);
            var uid = row.get("uid").toString();
            var data = (double) row.get("prob");
            idToProb.put(uid, data);
        }
        return idToProb;
    }
    */

}
