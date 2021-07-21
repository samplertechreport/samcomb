package com.samcomb.experiment.exp;

import com.samcomb.allocation.Strategy;
import com.samcomb.allocation.StrategyName;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.ExpConfigManager;
import com.samcomb.dataframe.Dataframe;
import com.samcomb.dataframe.MathTool;
import com.samcomb.dataframe.Row;
import com.samcomb.sampler.CEstimator;
import com.samcomb.sampler.CacheFetcher;
import com.samcomb.sampler.Estimator;
import com.samcomb.sampler.EstimatorInterface;
import com.samcomb.tool.QueryReader;
import com.samcomb.tool.Tool;
import com.samcomb.types.AggregateType;
import com.samcomb.types.Query;
import com.samcomb.types.ValueRank;
import com.samcomb.utils.Logger;
import com.samcomb.utils.Rander;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import static java.lang.Math.abs;
import static java.lang.Math.sqrt;

public class Experiment {

    private static long uidCounter = 0;
    private static boolean hasComputedSamplerInfo = false;
    public static HashMap<String, Double> dsCache = new HashMap<>();

    public static String getNextUid() {
        uidCounter += 1;
        return Long.toString(uidCounter);
    }

    public static Dataframe getSamplerQuality(Collection<Estimator> samplers, int qid) {
        var df = new Dataframe();
        for (var sampler : samplers) {
            var row = new Row();
            row.put("qid", qid);
            row.put("sampler", sampler.getName());
            row.put("realVarz", sampler.getRealVarz());
            df.insert(row);
        }
        return df;
    }

    public static Dataframe getSamplerDsDf(Collection<Estimator> estimatorList) {
        var df = new Dataframe();
        for (var estimator : estimatorList) {
            var row = new Row();
            row.put("samplerName", estimator.getName());
            row.put("ds", estimator.info.ds);
            df.insert(row);
        }
        return df;
    }

    public static void changeRandomSeed(Collection<? extends EstimatorInterface> estimatorList, int tryId,
                                        boolean impactWithDraw) {
        for (var estimator : estimatorList) {
            changeRandomSeed(estimator, tryId, impactWithDraw);
        }
    }

    public static void changeRandomSeed(EstimatorInterface estimator, int tryId, boolean impactWithdraw) {
        if (impactWithdraw) {
            var currandseed = estimator.getRandseed();
            estimator.setRandseed(currandseed - tryId - 1);
        } else {
            var oldrandseed = estimator.getRandseed();
            estimator.setRandseed(oldrandseed + tryId + 1);
        }
    }

    @Deprecated // moved to each sampler.
    private static double computeVarzEst(Map<String, List<Double>> df) {
        if (!(df.containsKey("prob") && df.containsKey("data") && df.containsKey("pred")))
            throw new IllegalStateException("data or prob or pred column does not exists");
        var dataList = df.get("data");
        var probList = df.get("prob");
        var predList = df.get("pred");
        if (!(dataList.size() == probList.size() && probList.size() == predList.size())) {
            throw new IllegalStateException(String.format(
                    "the size for data, prob, and pred list are not equal, data size:%d, prob size:%d, pred size:%d",
                    dataList.size(), probList.size(), predList.size()));
        }
        int ntuples = dataList.size();
        if (ntuples == 0)
            Logger.logger.warn("actual fetched 0 rows from dataFetcher");

        if (ntuples <= 1)
            return 0.0;

        double zsum = 0.0;
        double zSqrSum = 0.0;
        for (int i = 0; i < dataList.size(); i++) {
            var data = dataList.get(i);
            var pred = Math.round(predList.get(i));
            var prob = probList.get(i);
            if (MathTool.equal(prob, 0.0)) {
                throw new IllegalStateException("prob cannot be 0!, prob=" + prob);
            }
            double zValue = data * pred / prob;
            zsum += zValue;
            zSqrSum += zValue * zValue;
        }

        double estimatedVarz = zSqrSum / (ntuples - 1.0) - (zsum * zsum) / (ntuples * (ntuples - 1.0));
        return estimatedVarz;
    }

    @Deprecated // moved to each sampler.
    private static double computeIdealVarzBound(Strategy strategy, HashMap<String, Estimator> samplers)
            throws SQLException {
        Logger.logger.info("computing ideal varzbound for {}", strategy.name.toString());
        // var jdbcUrl = config.getString("jdbc.url");
        // var schemaName = config.getString("schemaName");
        // var samplerColumns = config.getStringList("samplerColumns");
        // var samplerTypes = config.getStringList("samplerTypes");
        // int nsamplers = samplerColumns.size();

        int budget = (int) strategy.getPar("totalBudget");
        int batchSize = (int) strategy.getPar("batchSize");

        double idealVarzBound = 0.0;
        for (var s : samplers.values()) {
            // var samplerName = samplerTypes.get(si) + "_" + samplerColumns.get(si);
            var dataFetcher = ((CacheFetcher) s.dataFetcher).copy();
            int leftBudget = budget;
            while (leftBudget > 0) {
                int curBatchSize = Math.min(leftBudget, batchSize);
                var curBatch = dataFetcher.getNext(curBatchSize);
                double varz = computeVarzEst(curBatch);
                if (varz > idealVarzBound)
                    idealVarzBound = varz;
                leftBudget -= curBatchSize;
            }
        }
        Logger.logger.info("end of computing ideal varzbound for {}", strategy.name.toString());
        return idealVarzBound;
    }

    private static double getRealVarzBound(HashMap<String, Estimator> samplers) {
        double realVarzBound = 0.0;
        for (var s : samplers.values()) {
            realVarzBound = Math.max(realVarzBound, s.info.ds);
        }
        return realVarzBound;
    }

    private static int getMaxBudget(List<Strategy> stratigies) {
        int maxBudget = 0;
        for (var s : stratigies) {
            int b = Double.valueOf(s.getPar("totalBudget").toString()).intValue();
            if (b > maxBudget)
                maxBudget = b;
        }
        return maxBudget;
    }

    // note that the last element is virtual, and just used for computing the last
    // rank is filtered population size.
    public static ArrayList<ValueRank> precomputeSortedRanks(Connection conn, Query query) throws SQLException {
        var stat = conn.createStatement();
        var valueRanks = new ArrayList<ValueRank>();
        var sql = String.format("SELECT RANK() OVER (ORDER BY %s) as rank_no, %s FROM %s WHERE %s ORDER BY rank_no",
                query.aggColumn, query.aggColumn, query.tableName, query.predSql);

        var rankExists = new HashSet<Long>();
        Logger.logger.info(sql);
        var result = stat.executeQuery(sql);
        while (result.next()) {
            long rank = result.getLong("rank_no");
            double value = Double.valueOf(result.getString(query.aggColumn));
            if (!rankExists.contains(rank)) {
                rankExists.add(rank);
                ValueRank vr = new ValueRank(value, rank);
                valueRanks.add(vr);
            }
        }

        stat.close();
        return valueRanks;
    }

    // qcnt is the maximal possible rank.
    public static HashMap<String, Double> getRanks(double value, ArrayList<ValueRank> sortedValueRanks, double qCnt) {
        double minGap = Double.MAX_VALUE;
        int closedId = -1;
        for (int i = 0; i < sortedValueRanks.size(); i++) {
            var vr = sortedValueRanks.get(i);
            if (abs(vr.value - value) < minGap) {
                minGap = abs(vr.value - value);
                closedId = i;
            }
        }
        double minRank = sortedValueRanks.get(closedId).rank;
        double maxRank = closedId + 1 >= sortedValueRanks.size() ? qCnt : sortedValueRanks.get(closedId + 1).rank;
        double avgRank = (minRank + maxRank) / 2.0;
        var res = new HashMap<String, Double>();
        res.put("min", minRank);
        res.put("max", maxRank);
        res.put("avg", avgRank);
        return res;
    }

    public static boolean containsLcb(ArrayList<Strategy> stratigies) {
        var lcbset = new HashSet<StrategyName>() {
            {
                add(StrategyName.Lcb);
                add(StrategyName.IdealLcb);
                add(StrategyName.AdaptiveLcb);
            }
        };
        for (var s : stratigies) {
            if (lcbset.contains(s.name))
                return true;
        }
        return false;
    }

    public static void runOnSchema(int runId, ExpName expName, Config config, String schemaName, String resultFolder)
            throws Exception {
        int tryNum = 1;
        long randseed = Rander.rand.nextLong();

        var write_info_per_query = false;
        var resultDf = new Dataframe();
        var samplerQualityDf = new Dataframe();

        var jdbcUrl = config.getString("jdbc");
        Connection conn = DriverManager.getConnection(jdbcUrl);
        boolean usePrecomputeSample = config.getBoolean("usePrecomputeSample");
        var queryPath = config.getString("queryPath");
        var tableName = config.getString("populationTable");
        var samplerColumns = config.getStringList("samplerColumns");
        var samplerTypes = config.getStringList("samplerTypes");
        var queryAggregateType = (AggregateType) config.getObject("queryAggregateType");

        var strategies = ExpConfigManager.getStrategies(expName, config);
        int maxBudget = getMaxBudget(strategies);
        var containLcbStrategy = containsLcb(strategies);

        var queries = QueryReader.readPredAndGetQueries(queryPath, queryAggregateType);
        var queryFileName = new File(queryPath).getName();
        FileUtils.copyFile(new File(queryPath), new File(resultFolder + "/" + queryFileName));

        // var runQueryNum =
        // (DefaultExpConfig.getEnvironment().equals(Environment.Remote))?
        // queries.size(): 2;
        var runQueryNum = queries.size();
        // runQueryNum = 2;
        for (var qid = 0; qid < runQueryNum; qid++) {
            var query = queries.get(qid);
            query.tableName = tableName;
            Dataframe currentQueryRes = new Dataframe();

            var realValue = Double.valueOf(Dataframe.getSingleValueFromSql(query.getSql(), conn, true).toString());
            if (MathTool.equal(realValue, 0.0))
                continue;

            var realValueRanks = new HashMap<String, Double>();
            // realPercentileRanks records mapping between rank and value. Used to compute
            // the rank error of percentile query estimation.
            var precomputedRanks = new ArrayList<ValueRank>();
            double qCnt = Double.NaN;
            if (query.aggFunction.equals("max") || query.aggFunction.equals("median")) {
                qCnt = Double
                        .valueOf(Dataframe
                                .getSingleValueFromSql(
                                        "SELECT COUNT(*) FROM " + query.tableName + " WHERE " + query.predSql, conn, true)
                                .toString());
                precomputedRanks = precomputeSortedRanks(conn, query);
                realValueRanks = getRanks(realValue, precomputedRanks, qCnt);
            }

            LinkedHashMap<String, Estimator> estimatorList;
            if (usePrecomputeSample) {
                var loadDataInMemory = (!expName.equals(ExpName.compareOfflineOptimalAndOnlineOptimalSampler));
                estimatorList = SamplerBuilder.createEstimatiorListUsePrecomputeSample(runId, samplerColumns,
                        samplerTypes, query, jdbcUrl, schemaName, loadDataInMemory, maxBudget);
            } else {
                estimatorList = SamplerBuilder.createEstimatiorListUseCatchPopu(samplerColumns, samplerTypes, query,
                        conn);
            }

            for (var estimator : estimatorList.values()) {
                // Logger.logger.info("computing info(ds, realValue, realVarz) for sampler:{}",
                // estimator.samplerName);
                if (query.aggFunction.equals("max") || query.aggFunction.equals("median")) {
                    estimator.info.setRealResult(realValue);
                } else {

                    double ds = Double.NaN;
                    if (containLcbStrategy) {
                        var mSql = String.format("SELECT MAX(%s/%s) AS m FROM %s", query.aggColumn,
                                estimator.samplerName.toLowerCase(), estimator.schemaName + "." + "population");
                        double m = Double.valueOf(Dataframe.getSingleValueFromSql(mSql, conn, true).toString());
                        ds = m * m / 4.0;
                    }
                    estimator.computeAndSetSamplerInfo(realValue, ds);
                }
            }

            if ((!hasComputedSamplerInfo) && (!query.aggFunction.equals("max"))
                    && (!query.aggFunction.equals("median"))) {
                hasComputedSamplerInfo = true;
                var samplerDsDf = getSamplerDsDf(estimatorList.values());
                samplerDsDf.save(String.format("%s/samplerDs.csv", resultFolder));
            }

            var curQualityDf = getSamplerQuality(estimatorList.values(), qid);
            samplerQualityDf.insert(curQualityDf);

            var cestimator = new CEstimator(estimatorList, randseed);

            // the random seed is different for different queries. But for the same query,
            // it is the same for different stratigies.
            cestimator.setRandseed(Rander.rand.nextLong());

            // var bestEstimatorName = cestimator.getRealBestEstimator().samplerName;
            for (int tryId = 0; tryId < tryNum; tryId += 1) {

                if (!usePrecomputeSample)
                    changeRandomSeed(estimatorList.values(), tryId, false);

                for (var strategy : strategies) {

                    int budget = (int) strategy.getPar("totalBudget");
                    Logger.logger.info(String.format(
                            "running runId:%d, qid:%d, tryId: %d, budget:%s,  initSize:%s, strategyName:%s", runId, qid,
                            tryId, strategy.getPar("totalBudget"), strategy.getPar("initSize"),
                            strategy.name.toString()));

                    // double idealVarzBound = (strategy.name == StrategyName.IdealLcb)?
                    // computeIdealVarzBound(strategy, estimatorList): Double.NaN;
                    // double realVarzBound = getRealVarzBound(estimatorList);

                    cestimator.reset();
                    if (!usePrecomputeSample)
                        changeRandomSeed(cestimator, tryId, false);
                    var uid = getNextUid();
                    cestimator.setUid(uid);
                    cestimator.setQuery(query);
                    cestimator.setStrategy(strategy);
                    if (query.aggFunction.equals("median")) {
                        cestimator.setStoreValueWeights(true);
                    } else {
                        cestimator.setStoreValueWeights(false);
                    }
                    if (query.aggFunction.equals("median") || query.aggFunction.equals("max")) {
                        cestimator.setPrecomputedRanks(precomputedRanks);
                        cestimator.setqCnt(qCnt);
                    }

                    cestimator.query(budget);

                    var rltError = abs(cestimator.getEstimation() / realValue - 1.0);
                    double realCi = Double.NaN;
                    if (expName.equals(ExpName.compareOfflineOptimalAndOnlineOptimalSampler)) {
                        double realVarz = Double.valueOf(cestimator.getRealVarzState());
                        realCi = 1.96 * sqrt(realVarz / budget);
                    } else {
                        realCi = cestimator.computeRealCi();
                    }
                    double ciError = realCi / realValue;

                    if (!usePrecomputeSample)
                        changeRandomSeed(cestimator, tryId, true);

                    var twoStepChosenSampler = (strategy.name == StrategyName.TwoStepComb
                            || strategy.name == StrategyName.BlinkDB) ? cestimator.getTwoStepChosenSampler().getName()
                            : "None";

                    var row = new Row();
                    row.put("gid", query.groupId);
                    row.put("qid", query.qid);
                    row.put("uid", uid);
                    row.put("sql", query.getSql());
                    row.put("runId", runId);
                    row.put("tryId", tryId);
                    if (expName == ExpName.computeRealVarD) {
                        row.put("singleSamplerName", strategy.getOrElse("singleSamplerName", "None"));
                        row.put("realValue", realValue);
                        row.put("realVarzState", cestimator.getRealVarzState());
                    } else {
                        row.put("schemaName", schemaName);
                        row.put("tableName", tableName);
                        row.put("comments", strategy.getOrElse("comments", "None"));
                        row.put("onlineSamplers", cestimator.getOnlineSamplerNameState());
                        row.put("availableSamplers", cestimator.getAvailableSamplerNameState());
                        row.put("availableSamplerNum", cestimator.getAvailableSamplerNum());
                        row.put("allSamplers", cestimator.getAllSamplerNameState());
                        row.put("twoStepChosenSampler", twoStepChosenSampler);
                        row.put("onlineOptimalSampler", cestimator.getOnlineOptimalSampler());
                        row.put("offlineOptimalSampler", cestimator.getOfflineOptimalSampler());
                        row.put("bestSampler", cestimator.getRealBestEstimator());
                        row.put("worstSampler", cestimator.getRealWorstEstimator());
                        row.put("budget", budget);
                        row.put("paraRatio", strategy.getOrElse("exp.paraRatio", "None"));
                        row.put("initSize", strategy.getOrElse("initSize", "None"));
                        row.put("initSizeRate", strategy.getOrElse("initSizeRate", "None"));
                        row.put("epsC", strategy.getOrElse("decayConstant", "None"));
                        row.put("estimatorUid", cestimator.uid);
                        row.put("estimatorName", cestimator.getName());
                        row.put("strategy", strategy.name.toString());
                        row.put("idealVarzBound", cestimator.getIdealVarzBoundState());
                        row.put("realVarzBound", cestimator.getRealVarzBoundState());
                        row.put("singleSamplerName", strategy.getOrElse("singleSamplerName", "None"));
                        row.put("weightAllocation", strategy.getOrElse("weightAllocationStrategy", "None"));
                        row.put("sizeAllocationState", cestimator.getSampleSizeAllocationState());
                        row.put("estimation", cestimator.getEstimation());
                        row.put("ciEstimation", cestimator.getQueryCiEst());
                        row.put("realValue", realValue);
                        row.put("realCi", cestimator.computeRealCi());
                        row.put("weightState", cestimator.getWeightState());
                        row.put("realVarzState", cestimator.getRealVarzState());
                        row.put("estimatedVarzState", cestimator.getEstimatedVarzState());
                        row.put("selRowsState", cestimator.getSelRowsState());
                        row.put("rltError", rltError);
                        row.put("ciError", ciError);
                        if (query.aggFunction.equals("max") || query.aggFunction.equals("median")) {

                            var estimationRank = getRanks(cestimator.getEstimation(), precomputedRanks, qCnt);
                            row.put("realPredSelectRows", qCnt);
                            row.put("realValueRankMin", realValueRanks.get("min"));
                            row.put("realValueRankMax", realValueRanks.get("max"));
                            row.put("realValueRankAvg", realValueRanks.get("avg"));
                            row.put("realValueRankRatioMin", realValueRanks.get("min") / qCnt);
                            row.put("realValueRankRatioMax", realValueRanks.get("max") / qCnt);
                            row.put("realValueRankRatioAvg", realValueRanks.get("avg") / qCnt);

                            row.put("EstimationRankMin", estimationRank.get("min"));
                            row.put("EstimationRankMax", estimationRank.get("max"));
                            row.put("EstimationRankAvg", estimationRank.get("avg"));
                            row.put("EstimationRankRatioMin", estimationRank.get("min") / qCnt);
                            row.put("EstimationRankRatioMax", estimationRank.get("max") / qCnt);
                            row.put("EstimationRankRatioAvg", estimationRank.get("avg") / qCnt);
                            var rltRankError = abs(estimationRank.get("avg") / realValueRanks.get("avg") - 1.0);
                            row.put("rltRankError", rltRankError);
                        }
                    }

                    resultDf.insert(row);
                    currentQueryRes.insert(row);

                    var curIterationDf = cestimator.getIterationRecorder();
                    var recordIterState = (boolean) strategy.getPar("recordIterState");
                    if (recordIterState)
                        curIterationDf.save(String.format("%s/iterDf/gid%s_qid%s_uid%s_%s.csv", resultFolder,
                                query.groupId, query.qid, uid, strategy.name.toString()));
                }

                if (!usePrecomputeSample)
                    changeRandomSeed(estimatorList.values(), tryId, true);
            }

            if (write_info_per_query) {
                currentQueryRes.save(String.format("%s/result_per_query/%s_qid%d.csv", resultFolder, schemaName, qid));
            }
        }

        if (expName == ExpName.computeRealVarD) {
            resultDf.save(String.format("%s_real_result.csv", queryPath));
        }

        resultDf.show(100);
        samplerQualityDf.show(100);
        resultDf.save(String.format("%s/result.csv", resultFolder));
        samplerQualityDf.save(String.format("%s/samplerQuality.csv", resultFolder));
    }

    public static void run(ExpName expName) throws Exception {
        double startTime = System.currentTimeMillis();
        var config = ExpConfigManager.getExpConfig(expName);
        Logger.logger.info("reset running experiment:{}", expName);
        var sourceCodePath = "src";
        var currentExpRootFolder = String.format("%s/exp/%s_%s", DefaultExpConfig.defaultOutputFolder, expName,
                ExpTool.getDatePrefix());
        var codeFolder = new File(sourceCodePath);
        FileUtils.copyDirectory(codeFolder, new File(currentExpRootFolder + "/" + sourceCodePath));
        var schemaNames = config.getStringList("schemaNames");
        // var schemaNames = Arrays.asList("skew_s1_z2");
        for (var schema : schemaNames) {
            Logger.logger.info("run exp " + expName + " on schema " + schema);
            var resultFolder = currentExpRootFolder + "/" + schema;
            var subconfig = config.getConfig(schema);
            int nruns = DefaultExpConfig.nruns;

            List<Integer> runIds = Tool.arange(0, nruns, 1);
            // temp, for speed up.
            if (expName.equals(ExpName.varySampelerNum)) {
                runIds = Arrays.asList(1, 2);
            }
            Logger.logger.info("run ids: " + runIds.toString());
            for (int runId : runIds) {
                runOnSchema(runId, expName, subconfig, schema, resultFolder + "/run" + runId);
            }
        }
        Logger.logger.info("end running experiment:" + expName);
        double runTime = (System.currentTimeMillis() - startTime) / 1000;
        var runTimeDf = new Dataframe();
        Row row = new Row();
        row.put("exp_runtime(seconds)", runTime);
        runTimeDf.insert(row);
        runTimeDf.save(currentExpRootFolder + "/runTime.csv");
    }

}
