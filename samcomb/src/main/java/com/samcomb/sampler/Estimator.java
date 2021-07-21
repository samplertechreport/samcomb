package com.samcomb.sampler;

import com.google.common.collect.Sets;
import com.samcomb.dataframe.Dataframe;
import com.samcomb.dataframe.MathTool;
import com.samcomb.types.Query;
import com.samcomb.types.ValueWeight;
import com.samcomb.utils.Logger;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Estimator implements EstimatorInterface {

    public boolean cursorEnd = false;  //used to see whether current cursor is the end. I.e, no more tuples to be fetched.
    public String defineCursorSql;
    //  public Connection conn;
    public Query query;
    public Statement fetchStat;
    public String schemaName;
    public boolean usePrecompueSample;
    public String varzBoundType;
    public double idealVarzBound = Double.NaN;
    public double realVarzBound = Double.NaN;
    public double adaptiveVarzBound = Double.NaN;
    public double currBatchVarzEst = Double.NaN;

    public SampleDrawer<Double> sampleDrawer;
    public String samplerName;
    public ArrayList<Double> data;
    public ArrayList<Integer> satisifyPredicate;
    public ArrayList<Double> prob;
    //public ArrayList<Double> zValueSample;
    public ArrayList<Double> population;
    public double zValueSum = 0.0;
    public double zValueSrqSum = 0.0;
    public double currentMaxValue = -(Double.MAX_VALUE / 2); // used for max query.
    public long sampleSize = 0;
    public int batchNum = 0;
    public double batchSumOfEstimatedVarz = 0.0;
    public boolean storeValueWeights = false;
    public ArrayList<ValueWeight> valueWeights = new ArrayList<>();
    // public double ds = 0.0;
    public int selRows = 0;
    private long randseed = 0;
    private String jdbc;
    boolean chunkByChunk = true;
    // public double realResult = 0.0;
    // public double realVarz = 0.0;
    public EstimatorInfo info = new EstimatorInfo();
    public DataFetcher dataFetcher;
    // public boolean computeDs = true;
    public HashMap<String, Double> mSqlCache = new HashMap<String, Double>();

    private ArrayList<Double> zValuesOneChunk = new ArrayList<Double>();


    // variable with default values.
    public boolean withReplacement = true;
    // public int lcbChunkSize = 0;


    public Estimator() {
        super();
    }

    public Estimator(ArrayList<Double> data,
                     ArrayList<Integer> satisifyPredicate,
                     ArrayList<Double> prob,
                     long randSeed,
                     String samplerName) throws IOException {

        assert MathTool.equal(MathTool.sum(prob), 1.0);

        this.usePrecompueSample = false;
        this.data = data;
        this.satisifyPredicate = satisifyPredicate;
        this.prob = prob;
        this.randseed = randSeed;

        // var dataIndex = Utils.range(0, data.size());
        this.sampleDrawer = new SampleDrawer<Double>(data, prob, randSeed);
        this.samplerName = samplerName;
        //   this.zValueSample = new ArrayList<Double>();
        //   this.computeAndSetDsUsingCachedPopulation();
        //   this.computeAndSetEstimatorInfoFromCache();
    }

    public Estimator(String jdbcUrl, String samplerName, Query query, String schemaName, DataFetcher dataFetcher) throws SQLException {
        this.usePrecompueSample = true;
        this.schemaName = schemaName;
        this.query = query;
        this.samplerName = samplerName;
        this.dataFetcher = dataFetcher;
        this.jdbc = jdbcUrl;

        // this.zValueSample = new ArrayList<Double>();

        //  openCursor();

    }


    private void openCursor() throws SQLException {
        cursorEnd = false;
        var conn = DriverManager.getConnection(this.jdbc);
        this.fetchStat = conn.createStatement();

        Logger.logger.debug("open cursor, cursorEnd:{}, samplerName:{}", cursorEnd, samplerName);

        this.fetchStat.execute("BEGIN;");
        // Logger.logger.debug(samplerName);
        var samplerTableName = schemaName + "." + samplerName + "_data";
        this.defineCursorSql = String.format("DECLARE cur%s CURSOR FOR SELECT %s as data, CASE WHEN (%s) THEN 1 ELSE 0 END as pred, prob FROM %s ORDER BY randcol;", samplerName, query.aggColumn, query.predSql, samplerTableName);
        this.fetchStat.execute(defineCursorSql);
        Logger.logger.debug(defineCursorSql);
    }


    private void closeCursor() throws SQLException {
        Logger.logger.debug("close cursor, cursorEnd:{}, samplerName:{}", cursorEnd, samplerName);

        this.fetchStat.execute(String.format("CLOSE cur%s;", samplerName));
        this.fetchStat.execute("COMMIT");
        this.fetchStat.close();

    }


    public void computeAndSetSamplerInfo(double realValue, double ds) throws SQLException, IOException {
        if (!query.aggFunction.equals("sum")) {
            throw new IllegalArgumentException("non-sum query should not use this function to set sampler info. It should be directly set the real value.");
        }
        if (this.usePrecompueSample) {
            this.computeAndSetDsEstimatorInfoFromDb(realValue, ds);
        } else {
            this.computeAndSetEstimatorInfoFromCache();
        }
    }

    private void computeAndSetDsEstimatorInfoFromDb(double realValue, double ds) throws SQLException, IOException {
        var conn = DriverManager.getConnection(this.jdbc);
        var populationTable = schemaName + "." + "population";
        var probCol = samplerName.toLowerCase();

        // var sumqClause = String.format("SUM(CASE WHEN %s THEN %s ELSE 0.0 END) AS sumq", query.predSql, query.aggColumn);
        // var sqrsumqClause = String.format("SUM(CASE WHEN %s THEN (%s)*(%s)/%s ELSE 0.0 END) AS sqrsumq", query.predSql, query.aggColumn, query.aggColumn, probCol);
        // var sql = String.format("SELECT %s, %s, %s FROM %s", dsClause, sumqClause, sqrsumqClause, populationTable);

        double sumq = realValue;


        String sqrsumSql = String.format("SELECT SUM((%s)*(%s)/(%s)) FROM %s WHERE %s", query.aggColumn, query.aggColumn, probCol, populationTable, query.predSql);
        var sqrsumq = Double.valueOf(Dataframe.getSingleValueFromSql(sqrsumSql, conn, true).toString());
//        var stat = conn.createStatement();
//        var result = stat.executeQuery(sqrsumSql);
//        if (!result.next()) throw new IllegalStateException("no result");
//        double sqrsumq = Double.valueOf(result.getString("sqrsum"));
//        double qCnt = Double.valueOf(result.getString("cnt"));
//        stat.close();

        double realVarz = sqrsumq - sumq;
        this.info.setDs(ds)
                .setRealResult(sumq)
                .setRealVarz(realVarz);
        conn.close();
    }


//    public void setComputeDs(boolean computeds)
//    {
//        this.computeDs = computeds;
//    }

    public long getRandseed() {
        return this.randseed;
    }

    public void setRandseed(long randseed) {
        this.randseed = randseed;
        this.sampleDrawer.setRandSeed(randseed);
    }

    public void reset() throws SQLException {

        this.dataFetcher.reset();
        if (this.usePrecompueSample) {
            //     closeCursor();
            //     openCursor();
        } else {
            this.sampleDrawer.reset();
        }

        //  this.zValueSample = new ArrayList<>();
        zValuesOneChunk = new ArrayList<Double>();
        zValueSum = 0.0;
        zValueSrqSum = 0.0;
        sampleSize = 0;
        batchNum = 0;
        batchSumOfEstimatedVarz = 0.0;
        // ds = 0.0;
        selRows = 0;
        idealVarzBound = Double.NaN;
        realVarzBound = Double.NaN;
        adaptiveVarzBound = Double.NaN;
        valueWeights = new ArrayList<>();
        currentMaxValue = -(Double.MAX_VALUE / 2);
        //computeDs = true;
        //  randseed = 0;
        //realVarz = 0.0;
    }


    public void setVarzBoundType(String boundType) {
        var validBound = Sets.newHashSet("ideal", "real", "adaptive");
        if (!validBound.contains(boundType)) throw new IllegalArgumentException("unknown varzBound type:" + boundType);

        this.varzBoundType = boundType;
    }


//    public void setComputeDs(boolean computeDs){
//        this.computeDs = computeDs;
//    }


    private static double computeVarzEst(Map<String, List<Double>> df) {
        if (!(df.containsKey("prob") && df.containsKey("data") && df.containsKey("pred")))
            throw new IllegalStateException("data or prob or pred column does not exists");
        var dataList = df.get("data");
        var probList = df.get("prob");
        var predList = df.get("pred");
        if (!(dataList.size() == probList.size() && probList.size() == predList.size())) {
            throw new IllegalStateException(String.format("the size for data, prob, and pred list are not equal, data size:%d, prob size:%d, pred size:%d",
                    dataList.size(), probList.size(), predList.size())
            );
        }
        int ntuples = dataList.size();
        if (ntuples == 0) Logger.logger.warn("actual fetched 0 rows from dataFetcher");

        if (ntuples <= 1) return 0.0;

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

    public void computeAndSetIdealVarzBound(int totalBudget, int batchSize) {
        Logger.logger.info("computing ideal varzbound for {}", samplerName);

        double idealVarzBound = 0.0;
        // var samplerName = samplerTypes.get(si) + "_" + samplerColumns.get(si);
        var dataFetcher = ((CacheFetcher) this.dataFetcher).copy();
        int leftBudget = totalBudget;
        while (leftBudget > 0) {
            int curBatchSize = Math.min(leftBudget, batchSize);
            var curBatch = dataFetcher.getNext(curBatchSize);
            double varz = computeVarzEst(curBatch);
            if (varz > idealVarzBound) idealVarzBound = varz;
            leftBudget -= curBatchSize;
        }

        Logger.logger.info("end of computing ideal varzbound for {}", samplerName);
        this.idealVarzBound = idealVarzBound;
    }


    public void setRealVarzBound() {
        this.realVarzBound = this.info.ds;
    }

    public void setStoreValueWeights(boolean storeValueWeights) {
        this.storeValueWeights = storeValueWeights;
    }

    public Estimator setWithReplacement(boolean withReplacement) {
        this.withReplacement = withReplacement;
        return this;
    }

    /*
    public Estimator setLcbChunkSize(int lcbChunkSize) {
        this.lcbChunkSize = lcbChunkSize;
        return this;
    }
    */


    /*
    private void updateChunkStat(double zValue, boolean lastElement)
    {
        {
            zValuesOneChunk.add(zValue);
            if (lastElement && (zValuesOneChunk.size() >= 2)) {
            this.chunkNum += 1;
            Chunk chunk = new Chunk(zValuesOneChunk);
            this.chunkSumOfEstimatedVarz += Math.max(0.0, chunk.estimatedVarz);
            zValuesOneChunk = new ArrayList<>();
            }
        }
    }
    */


    public String getName() {
        return this.samplerName;
    }


    public void query(int budget) throws SQLException {
        reset();
        this.addSample(budget);
        reset();
    }


    public int addSample(int size) {
        var df = this.dataFetcher.getNext(size);
        if (!(df.containsKey("prob") && df.containsKey("data") && df.containsKey("pred")))
            throw new IllegalStateException("data or prob or pred column does not exists");
        var dataList = df.get("data");
        var probList = df.get("prob");
        var predList = df.get("pred");
        if (!(dataList.size() == probList.size() && probList.size() == predList.size())) {
            throw new IllegalStateException(String.format("the size for data, prob, and pred list are not equal, data size:%d, prob size:%d, pred size:%d",
                    dataList.size(), probList.size(), predList.size())
            );
        }
        int actualFetchedRows = dataList.size();

        if (actualFetchedRows == 0) Logger.logger.warn("actual fetched 0 rows from dataFetcher");


        double curSum = 0.0;
        double curSqrsum = 0.0;
        for (int i = 0; i < dataList.size(); i++) {
            var data = dataList.get(i);
            var pred = Math.round(predList.get(i));
            var prob = probList.get(i);
            if (MathTool.equal(prob, 0.0)) {
                throw new IllegalStateException("prob cannot be 0!, prob=" + prob);
            }
            double zValue = data * pred / prob;
            if (pred == 1) {
                this.currentMaxValue = Math.max(this.currentMaxValue, data);
                if (storeValueWeights) {
                    var vw = new ValueWeight(data, 1.0 / prob);
                    this.valueWeights.add(vw);
                }
            }

            this.sampleSize += 1;
            this.selRows += (int) pred;
            this.zValueSum += zValue;
            this.zValueSrqSum += zValue * zValue;
            curSum += zValue;
            curSqrsum += zValue * zValue;
        }

        if (actualFetchedRows >= 2) {

            this.batchNum += 1;
            double batchSize = actualFetchedRows;
            double batchVarzEst = curSqrsum / (batchSize - 1.0) - (curSum * curSum) / (batchSize * (batchSize - 1.0));
            this.currBatchVarzEst = batchVarzEst;

            if (Double.isNaN(adaptiveVarzBound)) {
                adaptiveVarzBound = realVarzBound;
            }

            if (this.selRows > 0) {
                if (MathTool.equal(adaptiveVarzBound, realVarzBound)) {
                    adaptiveVarzBound = batchVarzEst;
                } else {
                    adaptiveVarzBound = Math.max(adaptiveVarzBound, batchVarzEst);
                }
            }

            this.batchSumOfEstimatedVarz += batchVarzEst;
        }

        return actualFetchedRows;

        //  if (this.usePrecompueSample) return addSampleUsePrecomputedSample(size);
        //  else return addSampleUseSampleDrawer(size);
    }


    public double getRealVarz() {
        return this.info.realVarz;
    }


    public double getRealResult() {
        return this.info.realResult;
    }

    private void computeAndSetEstimatorInfoFromCache() {
        this.population = MathTool.multiply(this.data, this.satisifyPredicate);
        assert !this.population.isEmpty();
        double sqrsum = 0.0;
        double sum = 0.0;
        for (int i = 0; i < this.population.size(); i++) {
            sqrsum += Math.pow(this.population.get(i), 2.0) / prob.get(i);
            sum += this.population.get(i);
        }
        var realVarz = sqrsum - sum * sum;

        var z = MathTool.divide(data, prob);
        var m = MathTool.max(z);
        var ds = m * m / 4.0;
        this.info
                .setDs(ds)
                .setRealResult(sum)
                .setRealVarz(realVarz);
    }


    public double getEstimation() {
        if (this.sampleSize == 0) throw new IllegalStateException("sample size is 0, cannot return an estimation");
        return this.zValueSum / (double) this.sampleSize;
    }

    public double currentEstimatedVarz() {
        if (this.sampleSize == 0) return Double.NaN;

        if (this.chunkByChunk) return this.batchSumOfEstimatedVarz / batchNum;
        else {
            double sqrsum = this.zValueSrqSum;
            double sum = this.zValueSum;
            double n = this.sampleSize;
            return sqrsum / (n - 1.0) - (sum * sum) / (n * (n - 1.0));
        }
    }

    public double getVariance() {
        var varz = currentEstimatedVarz();
        return varz / this.sampleSize;
    }

    public double getCi() {
        double vari = getVariance();
        double ci = 1.96 * Math.sqrt(vari);
        return ci;
    }


    public double computeLcbOfVarz(double lcbWeight, String strategy, double t) throws Exception {
        double ci = 0.0;
        switch (strategy) {
            case "hoeffding":
                if (!chunkByChunk)
                    throw new IllegalArgumentException("to use hoeffding bound tuples must be allocated chunk by chunk");
                ci = hoeffdingVarzCi(t);
                break;
            case "cheby":
                ci = chebyshevCiOfVarz(t);
                break;
            default:
                throw new Exception("unknown lcb strategy:" + strategy);
        }
        // double ci = chunkByChunk()? hoeffdingVarzCi(t): chebyshevCiOfVarz(t);
        //double ci = getCiOfVarz();
        return this.currentEstimatedVarz() - lcbWeight * ci;
    }


    private double hoeffdingVarzCi(double t) {
        double varzBound = 0.0;
        switch (varzBoundType) {
            case "ideal": {
                varzBound = idealVarzBound;
                break;
            }
            case "real": {
                varzBound = realVarzBound;
                break;
            }
            case "adaptive": {
                varzBound = adaptiveVarzBound;
                break;
            }

        }
        return varzBound * Math.sqrt(Math.log(t) / batchNum);
    }

    //default: sqrt(20), represents a 95% confidence.
    private double chebyshevCiOfVarz(double t) {
        //  return 0.0;
        //  double sigma = 1.0 / Math.pow(Math.log(t), 2.0);
        double sigma = 1.0 / (t * Math.pow(Math.log(t), 2.0));
        //  sigma = Math.min(sigma / 0.01, 1.0);
        //double sigma = 1.0/ t;


        // double sigma = Math.pow(1.0 / this.t, 2.0);
        // double sigma = 1.0/ Math.pow(t, 1.0001);
        // double sigma = 1.0 / (t * Math.log(t));
        double varOfVarz = equationVarianceOfVarz();
        //double varOfVarz = bootstrapVarianceOfVarz();
        double ci = Math.sqrt(varOfVarz / sigma);
        return ci;
    }


    private double equationVarianceOfVarz() {
        //TODO: to implement.

        return 0.0;
        /*
        double avg = Tool.mean(zValueSample);
        double u2 = 0.0;
        double u4 = 0.0;
        double n = sampleSize;
        for (double v: zValueSample)
        {
            u2 += Math.pow(v - avg, 2.0);
            u4 += Math.pow(v - avg, 4.0);
        }
        u2 = u2/ ( n - 1.0);
        u4 = u4/(n - 3.0);
        double w = (n - 3.0)/(n - 1.0);
        double varianceOfvarz = (u4 - w*Math.pow(u2, 2.0))/n;
        return varianceOfvarz;
         */
    }


}
