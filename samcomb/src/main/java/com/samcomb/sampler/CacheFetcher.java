package com.samcomb.sampler;

import com.samcomb.types.Query;
import com.samcomb.utils.Logger;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheFetcher implements DataFetcher {

    public class DataPredProbLists {
        public ArrayList<Double> dataList;
        public ArrayList<Double> predList;
        public ArrayList<Double> probList;

        public DataPredProbLists(ArrayList<Double> dataList, ArrayList<Double> predList, ArrayList<Double> probList) {
            if (dataList.size() != predList.size() || dataList.size() != probList.size()) {
                throw new IllegalArgumentException("data, pred, and prob size should be equal");
            }
            this.dataList = dataList;
            this.predList = predList;
            this.probList = probList;
        }
    }

    // sql -> dataprepproblists. Need to take care of the space. 
    // Mainly used for multiple samplers exp. Only one query cache is allowed. Will check query id using currQueryId;
    public static HashMap<String, DataPredProbLists> queryCache = new HashMap<String, DataPredProbLists>();
    public static String currQueryId = "None";

    private List<Double> dataList;
    private List<Double> predList;
    private List<Double> probList;
    int startId = 0;
    int totalRows = 0;


    public CacheFetcher() {
        super();
    }

    public CacheFetcher(List<Double> dataList, List<Double> predList, List<Double> probList) {
        if (dataList.size() != predList.size())
            throw new IllegalArgumentException("dataList size not equal to predList");
        if (predList.size() != probList.size())
            throw new IllegalArgumentException("predList size not equal to probList");
        this.dataList = dataList;
        this.predList = predList;
        this.probList = probList;
        this.startId = 0;
        this.totalRows = dataList.size();
    }

    public CacheFetcher(int runId, String jdbcUrl, String samplerName, Query query, String schemaName, int maxBudget) throws SQLException {
        var samplerTableName = schemaName + "." + samplerName + "_sample_run" + runId;
        var sql = String.format(
                "SELECT %s as data, CASE WHEN (%s) THEN 1.0 ELSE 0.0 END as pred, prob FROM %s ORDER BY randcol LIMIT %d;",
                query.aggColumn, query.predSql, samplerTableName, maxBudget + 10);

        if (queryCache.containsKey(sql)) {
            Logger.logger.info("used cached result: building cacheFetcher for sampler:{}, running sql:{}", samplerName,
                    sql);
            var dataPredProbLists = queryCache.get(sql);
            this.dataList = dataPredProbLists.dataList;
            this.predList = dataPredProbLists.predList;
            this.probList = dataPredProbLists.probList;
            this.totalRows = this.dataList.size();
            return;
        }


        var conn = DriverManager.getConnection(jdbcUrl);
        var stat = conn.createStatement();
        Logger.logger.info("building cacheFetcher for sampler:{}, running sql:{}", samplerName, sql);
        var rs = stat.executeQuery(sql);

        var dataList = new ArrayList<Double>();
        var predList = new ArrayList<Double>();
        var probList = new ArrayList<Double>();

        while (rs.next()) {
            var data = rs.getDouble("data");
            var prob = rs.getDouble("prob");
            var pred = rs.getDouble("pred");
            dataList.add(data);
            probList.add(prob);
            predList.add(pred);
        }
        stat.close();
        conn.close();

        if (!query.qid.equals(currQueryId)) {
            Logger.logger.info("clear cached cachefeather for query:" + currQueryId);
            queryCache = new HashMap<>();
            currQueryId = query.qid;
        }

        var dataPredProbLists = new DataPredProbLists(dataList, predList, probList);
        queryCache.put(sql, dataPredProbLists);

        this.dataList = dataList;
        this.predList = predList;
        this.probList = probList;
        this.totalRows = dataList.size();

    }


    public CacheFetcher copy() {
        return new CacheFetcher(this.dataList, this.predList, this.probList);
    }


    @Override
    public void reset() {
        this.startId = 0;
    }

    public Map<String, List<Double>> getNext(int size) {
        int endId = Math.min(this.startId + size, totalRows);
        var curData = dataList.subList(startId, endId);
        var curProb = probList.subList(startId, endId);
        var curPred = predList.subList(startId, endId);

        this.startId = endId;

        var res = new HashMap<String, List<Double>>();
        res.put("data", curData);
        res.put("prob", curProb);
        res.put("pred", curPred);
        return res;
    }

}
