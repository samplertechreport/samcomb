package com.samcomb.experiment.exp;

import com.samcomb.config.Config;
import com.samcomb.config.ExpConfigManager;
import com.samcomb.dataframe.Dataframe;
import com.samcomb.dataframe.MathTool;
import com.samcomb.dataframe.Row;
import com.samcomb.tool.Tool;
import com.samcomb.utils.Logger;
import com.samcomb.utils.Rander;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class QueryGenerator {

    private Connection conn;
    private List<String> aggColumns;
    private String aggFunction;
    private String samplePath;
    private String sampleName;
    private String tableName;
    private double sampleRate;
    private Dataframe sampledf;
    private HashSet<String> categoricalColumns;
    // private  String aggColName;
    private List<String> predColNames;
    private int queryNum;
    private double minSelectively;
    private double maxSelectively;
    private List<Double> minSamplerDiffs;
    private List<Double> maxSamplerDiffs;
    private List<String> additionalPredicates;
    private String savePath;
    private List<String> samplerColumns;
    private List<String> samplerTypes;
    // private String additionalPredicate = "true";
    // private ForcePredicate forpredicate = new ForcePredicate();


    public class QueryInfo {
        public String bestSampler;
        public String aggCol;
        public int groupId;
        public String aggFunc;
        public String predSql;
        public double selectivity;
        public String varzStat;
        public long selectedRows;
        public long totalRows;
        public double samplerDiff;
    }


    public QueryGenerator(Config config) throws SQLException {
        var jdbc = config.getString("jdbc");
        this.conn = DriverManager.getConnection(jdbc);
        this.savePath = config.getString("savePath");
        this.tableName = config.getString("tableName");
        this.predColNames = config.getStringList("predicateColumns");
        this.queryNum = config.getInt("queryNum");
        this.minSelectively = config.getDouble("minSelectively");
        this.maxSelectively = config.getDouble("maxSelectively");
        this.sampleRate = config.getDouble("sampleRate");
        this.aggColumns = config.getStringList("aggregateColumns");
        this.aggFunction = config.getString("aggregateFunction");
        this.additionalPredicates = config.getStringList("additionalPredicates");
        this.samplerColumns = config.getStringList("samplerColumns");
        this.samplerTypes = config.getStringList("samplerTypes");
        this.minSamplerDiffs = config.getDoubleList("minSamplerDiffs");
        this.maxSamplerDiffs = config.getDoubleList("maxSamplerDiffs");
        if (this.samplerColumns.size() != this.samplerTypes.size())
            throw new IllegalArgumentException("sampler column size does not equal to sampler type size");
        if (this.minSamplerDiffs.size() != this.maxSamplerDiffs.size())
            throw new IllegalArgumentException("minSamplerDiffs size should be equal to maxSamplerDiffs size");
        if (this.minSamplerDiffs.size() != this.additionalPredicates.size())
            throw new IllegalArgumentException("minSamplerDiffs size should be equal to additionalPredicates size");

    }


    private void readSample() throws SQLException {

        var stat = conn.createStatement();
        stat.execute("CREATE EXTENSION IF NOT EXISTS pg_prewarm;");
        stat.execute(" SELECT setseed(0.5);");

        var schemaName = tableName.split("\\.")[0];
        this.sampleName = MathTool.equal(sampleRate, 1.0) ? tableName : schemaName + "." + "uf_sample_" + Math.round(sampleRate * 10000) + "perper";

        Logger.logger.info("creating uf sample with rate:{}", sampleRate);
        stat.execute(String.format("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s WHERE random() < %f", sampleName, tableName, sampleRate));

        Logger.logger.info("caching sample into cache");
        stat.execute(String.format("SELECT pg_prewarm('%s');", sampleName));

        //  this.populationSize = Dataframe.getSingleValueFromSql(String.format("SELECT COUNT(*) as cnt FROM %s", tableName), conn);


        //  var sql = String.format("SELECT * FROM %s WHERE random() < %f and %s", tableName, sampleRate, additionalPredicate);
        //  Logger.logger.debug(sql);
        // var sample = conn.createStatement().executeQuery(sql);

        //   this.sampledf = Dataframe.fromSql(conn, sql);
        //   Logger.logger.debug("sample size:" + sampledf.getRowNum());
//        this.sampledf.show(50);
        Logger.logger.info("load sample successfully");
    }

    private HashMap<String, ArrayList<Object>> getDistinctValues() throws IOException, SQLException {
        HashMap<String, HashSet<Object>> uniqueValuesSet = new HashMap<>();
        categoricalColumns = new HashSet<>();
        for (String col : predColNames) {
            var sql = String.format("SELECT DISTINCT %s as dist FROM %s", col, sampleName);
            var df = Dataframe.fromSql(conn, sql).get("dist");
            if (df.isCategorical()) categoricalColumns.add(col);
            var distinct = df.distinct();
            uniqueValuesSet.put(col, distinct);
        }

        HashMap<String, ArrayList<Object>> distinctValues = new HashMap<>();
        for (var entry : uniqueValuesSet.entrySet()) {
            distinctValues.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }

        return distinctValues;
    }


    private boolean predicateSatisified(QueryInfo queryInfo, HashSet<Long> selSet, HashMap<String, Integer> samplerQueryCount, int groupId, long totalCnt) throws IOException, SQLException {

        boolean equalBest = false;
        if (equalBest) {
            int queryEachSampler = queryNum / samplerQueryCount.size();
            int nSatisifiedSampler = 0;
            for (var s : samplerQueryCount.keySet()) {
                if (samplerQueryCount.get(s) >= queryEachSampler) nSatisifiedSampler += 1;
            }
            if (nSatisifiedSampler >= samplerQueryCount.size()) return false;
        }


        // Logger.logger.info("here, sql:{}", queryInfo.predSql);


        double minSamplerDiff = this.minSamplerDiffs.get(groupId);
        double maxSamplerDiff = this.maxSamplerDiffs.get(groupId);

//        var sj = new StringJoiner(",");
//        var sumqClause = String.format("SUM(CASE WHEN (%s) THEN %s ELSE 0.0 END) AS sumq", queryInfo.predSql, queryInfo.aggCol);
//        var cntqClause = String.format("SUM(CASE WHEN (%s) THEN 1 ELSE 0 END) as cntq", queryInfo.predSql);
//        var totalcntClause = "COUNT(*) as total_cnt";
//        sj.add(sumqClause);
//        sj.add(cntqClause);
//        sj.add(totalcntClause);
//
//        int nsampler = this.samplerColumns.size();
//        for (int i=0; i< nsampler; i++)
//        {
//            var probCol = samplerTypes.get(i) + "_" + samplerColumns.get(i);
//            var sqrsumqClause = String.format("SUM(CASE WHEN %s THEN (%s)*(%s)/%s ELSE 0.0 END) AS sqrsumq_%s", queryInfo.predSql, queryInfo.aggCol, queryInfo.aggCol, probCol, probCol);
//            sj.add(sqrsumqClause);
//        }
//
//        var sql = String.format("SELECT %s FROM %s", sj.toString(), sampleName);
//        var res = Dataframe.fromSql(conn, sql);
//        var total_cnt = ((Number) res.get("total_cnt").get(0)).longValue();
//        var pred_cnt = ((Number) res.get("cntq").get(0)).longValue();


        var sj = new StringJoiner(",");
        var sumqClause = String.format("SUM(%s) AS sumq", queryInfo.aggCol);
        var cntqClause = String.format("COUNT(*) AS pred_cnt");
        sj.add(sumqClause);
        sj.add(cntqClause);

        int nsampler = this.samplerColumns.size();
        for (int i = 0; i < nsampler; i++) {
            var probCol = samplerTypes.get(i) + "_" + samplerColumns.get(i);
            var sqrsumqClause = String.format("SUM((%s)*(%s)/%s) AS sqrsumq_%s", queryInfo.aggCol, queryInfo.aggCol, probCol, probCol);
            sj.add(sqrsumqClause);
        }

        var sql = String.format("SELECT %s FROM %s WHERE %s", sj.toString(), sampleName, queryInfo.predSql);
        Logger.logger.debug(sql);
        var res = Dataframe.fromSql(conn, sql);
        var pred_cnt = ((Number) res.get("pred_cnt").get(0)).longValue();


        double curSel = (double) pred_cnt / (double) totalCnt;
        queryInfo.selectedRows = pred_cnt;
        queryInfo.totalRows = totalCnt;
        queryInfo.selectivity = curSel;
        if (pred_cnt == 0 || selSet.contains(pred_cnt)) return false;
        if (curSel < minSelectively || curSel > maxSelectively) return false;

        var varzJoiner = new StringJoiner(",");
        double sumq = ((Number) res.get("sumq").get(0)).doubleValue();
        // Logger.logger.info("sumq:{}", sumq);
        //  res.show();
        var varzList = new ArrayList<Double>();
        double minVarz = Double.MAX_VALUE;
        String bestSampler = samplerColumns.get(0);
        for (int i = 0; i < nsampler; i++) {
            var samplerName = samplerTypes.get(i) + "_" + samplerColumns.get(i);
            var sqrsumq = ((Number) res.get("sqrsumq_" + samplerName.toLowerCase()).get(0)).doubleValue();
            double realVarz = sqrsumq - sumq;
            if (realVarz < minVarz) {
                minVarz = realVarz;
                bestSampler = samplerColumns.get(i);
            }
            varzList.add(realVarz);
            varzJoiner.add(String.format("'%s':%f", samplerName, realVarz));
        }
        Collections.sort(varzList);

        if (MathTool.equal(varzList.get(0), 0.0)) return false;
        var samplerDiff = Math.sqrt(varzList.get(1) / varzList.get(0));

        queryInfo.bestSampler = bestSampler;
        queryInfo.samplerDiff = samplerDiff;
        queryInfo.varzStat = String.format("\"%s\"", varzJoiner.toString());

        if (equalBest) {
            int queryEachSampler = queryNum / samplerQueryCount.size();
            if (!samplerQueryCount.containsKey(bestSampler)) return false;
            if (samplerQueryCount.get(bestSampler) >= queryEachSampler) return false;
        }

        if (samplerDiff < minSamplerDiff || samplerDiff > maxSamplerDiff) return false;

        if (equalBest) {
            var cc = samplerQueryCount.get(bestSampler);
            samplerQueryCount.put(bestSampler, cc + 1);
        }

        selSet.add(pred_cnt);
        return true;
    }


    public static void run(ExpName expName) throws IOException, SQLException {
        var config = ExpConfigManager.getExpConfig(expName);
        for (var s : config.getStringList("schemaNames")) {
            Logger.logger.info("generate query for schema:" + s);
            var subConfig = config.getConfig(s);
            var queryGenerator = new QueryGenerator(subConfig);
            queryGenerator.generateAndSave();
        }
    }

    public void generateAndSave() throws IOException, SQLException {
        readSample();
        var distinctValuesForColumns = getDistinctValues();
        var selSet = new HashSet<Long>();
        var resDf = new Dataframe();
        int ngroup = this.additionalPredicates.size();

        for (int i = 0; i < ngroup; i++) {
            var curDf = generatePredicate(distinctValuesForColumns, selSet, i);
            resDf.insert(curDf);
        }
        resDf.save(this.savePath);
    }

    private Dataframe generatePredicate(HashMap<String, ArrayList<Object>> distinctValuesForColumns, HashSet<Long> selSet, int groupId) throws IOException, SQLException {

        String additionalPredicate = this.additionalPredicates.get(groupId);

        var resdf = new Dataframe();

        var samplerQueryCount = new HashMap<String, Integer>();
        for (var s : samplerColumns) {
            if (!s.toLowerCase().equals("none")) {     //do not consider uniform sampler is best.
                samplerQueryCount.put(s, 0);
            }
        }


        var totalCntSql = "SELECT COUNT(*) FROM " + sampleName;
        long totalCnt = (long) Dataframe.getSingleValueFromSql(totalCntSql, conn, true);

        //  long minSelRows = Math.max(1, (long) (populationSize*sampleRate * minSelectively));
        //  long maxSelRows = (long) (populationSize*sampleRate * maxSelectively);

        //  Logger.log("minSelRows:", minSelRows);
        //  Logger.log("maxSelRow:", maxSelRows);
        // var predSqls = new ArrayList<String>();
        //  var sqlSel = new ArrayList<Double>();

        // var colNameList = new ArrayList<String>(predColNames);
        Logger.logger.info("org queryNum:{}", queryNum);
        // queryNum = queryNum/samplerColumns.size()*samplerColumns.size();
        Logger.logger.info("after process queryNum:{}", queryNum);

        int maxIterPerQuery = 200;
        for (int getQueryNum = 0; getQueryNum < queryNum; getQueryNum += 1) {
            Logger.logger.info("getQueryNum:" + getQueryNum);
            long iter = 0;
            // long curSelRows = 0;
            //  var curPredicate = new HashMap<String, NamedPredicate<Object>>();
            // Query curQuery = new Query();
            QueryInfo curQuery = new QueryInfo();
            String curPredSql = "";
            boolean curPredSatisified = false;
            while (iter <= maxIterPerQuery && (!curPredSatisified)) {
                //   var predicate = new HashMap<String, NamedPredicate<Object>>();
                var predSql = new ArrayList<String>();
                iter += 1;
                int curColNum = Rander.rand.nextInt(predColNames.size()) + 1;
                //int curColNum = 2;
                var colNameList = Tool.randomItems(predColNames, curColNum);
                for (int cid = 0; cid < curColNum; cid++) {
                    var colName = colNameList.get(cid);
                    var colValues = distinctValuesForColumns.get(colName);
                    // String colName = col.getKey();
                    // var colValues = col.getValue();
                    var p1 = Rander.rand.nextInt(colValues.size());
                    if (categoricalColumns.contains(colName)) {
                        var stringValue = colValues.get(p1).toString();
                        /*
                        if ((forpredicate.colName != null) && forpredicate.colName.equals(colName))
                        {
                            stringValue = forpredicate.category;
                        }
                        */

                        String category = stringValue;
                        var predStr = String.format("%s = '%s'", colName, category);
                        // var namedPre = new NamedPredicate<Object>(predStr, x -> (((String) x).equals(category)));
                        // predicate.put(colName, namedPre);
                        predSql.add(predStr);// predStr;
                    } else {
                        var p2 = Rander.rand.nextInt(colValues.size());

                        double v1 = Double.valueOf(colValues.get(p1).toString());
                        double v2 = Double.valueOf(colValues.get(p2).toString());

                        double lb = Math.min(v1, v2);
                        double ub = Math.max(v1, v2);
                        double lbb = lb;
                        double ubb = ub;

                        int option = Rander.rand.nextInt(2);
                        String predStr;
                        switch (option) {
                            case 0: {
                                predStr = String.format("%s <= %f", colName, lbb);
                                break;
                            }
                            case 1: {
                                predStr = String.format("%s >= %f", colName, ubb);
                                break;
                            }
                            case 2: {
                                predStr = String.format("%s >= %f and %s <= %f", colName, lbb, colName, ubb);
                                break;
                            }
                            default:
                                throw new IllegalStateException("unknown random option:" + option);
                        }

                        // var predStr = String.format("%s >= %f and %s <= %f", colName, lbb, colName, ubb);
                        //   var namedPre = new NamedPredicate<Object>(predStr, x -> {
                        //       var d = Double.valueOf(x.toString());
                        //       return (d >= lbb) && (d <= ubb);
                        //       (Double.valueOf(x.toString())) >= lb && ((Double) x) <= ub
                        //   });
                        //   predicate.put(colName, namedPre);
                        predSql.add(predStr);
                    }
                    //predicate.put(colName, x -> (x >= lb && x <= ub));
                }
                // curPredicate = predicate;
                curPredSql = String.join(" and ", predSql);
                if (!(additionalPredicate.equals("true") || additionalPredicate.equals(""))) {
                    curPredSql += " and " + "(" + additionalPredicate + ")";
                }

                curQuery.aggFunc = aggFunction;
                curQuery.aggCol = Tool.randomItem(aggColumns);
                curQuery.predSql = curPredSql;
                curQuery.groupId = groupId;
                curQuery.samplerDiff = Double.NaN;

                curPredSatisified = predicateSatisified(curQuery, selSet, samplerQueryCount, groupId, totalCnt);


                Logger.logger.info(String.format("generating query %s with sel: %f, groupId:%d, samplerDiff: %f, bestSampler:%s, sql: SELECT %s(%s) WHERE %s", getQueryNum, curQuery.selectivity, curQuery.groupId, curQuery.samplerDiff, curQuery.bestSampler, curQuery.aggFunc, curQuery.aggCol, curQuery.predSql));
            }
            if (curPredSatisified) {
                // selSet.add(curSelRows);
                Logger.logger.info(String.format("find query %s with sel: %f, sql: SELECT %s(%s) WHERE %s, samplerDiff: %f", getQueryNum, curQuery.selectivity, curQuery.aggFunc, curQuery.aggCol, curQuery.predSql, curQuery.samplerDiff));

                Row row = new Row();
                row.put("groupId", curQuery.groupId);
                row.put("aggCol", curQuery.aggCol);
                row.put("aggFunc", curQuery.aggFunc);
                row.put("pred", curQuery.predSql);
                row.put("selectivity", curQuery.selectivity);
                row.put("selectedRows", curQuery.selectedRows);
                row.put("totalRows", curQuery.totalRows);
                row.put("varzSata", curQuery.varzStat);
                row.put("bestSampler", curQuery.bestSampler);
                row.put("samplerDiff", curQuery.samplerDiff);
                row.put("gen_from_table", sampleName);
                //Logger.logger(row.toString());
                resdf.insert(row);
                //  predicates.add(curPredicate);
            }
        }

        resdf.show(10);

        return resdf;



        /*
        var qid=0;
        Logger.log("print predicate key set");

        var res = new ArrayList<String>();
        for (int i=0; i< predSqls.size(); i++)
        {
            var s = predSqls.get(i) + ", " + sqlSel.get(i);
            res.add(s);
        }


        for (var pre: predicates) {
            //Logger.log(pre);
            queries.add(new Query(qid, aggColName,pre));
            qid+=1;
        }

        return res;
        */
    }
}
