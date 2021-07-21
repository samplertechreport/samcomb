package com.samcomb.sampler;

import com.samcomb.allocation.Strategy;
import com.samcomb.allocation.StrategyName;
import com.samcomb.dataframe.Dataframe;
import com.samcomb.dataframe.Row;
import com.samcomb.experiment.exp.Experiment;
import com.samcomb.types.Query;
import com.samcomb.types.ValueRank;
import com.samcomb.types.ValueWeight;
import com.samcomb.utils.Logger;
import com.samcomb.utils.Timer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Random;
import java.util.*;

import static com.samcomb.allocation.StrategyName.*;
import static java.lang.Math.abs;

public class CEstimator implements EstimatorInterface {

    public String resultFolder;
    public Timer timer = new Timer();
    public String uid;
    //public int qid;
    public Query query;
    private long randseed;
    // public double idealVarzBound = Double.NaN;
    // public double realVarzBound = Double.NaN;
    private Random rand;
    private double t = 0.0;
    private String epsCurrChosenApproach;
    private double currEps;

    private LinkedHashSet<String> availableSamplers;
    private LinkedHashSet<String> onlineSamplers;
    private LinkedHashMap<String, Estimator> allSamplers;
    private Dataframe iterationRecorder = new Dataframe();  //key:field, value: recorded value.
    private Dataframe result = new Dataframe();

    private double queryResultEst = -1;
    private double estimatedVarOfCsampler = -1;
    private double queryCiEst = -1;
    private LinkedHashMap<String, Double> samplerWeight = new LinkedHashMap<String, Double>();
    private Strategy allocationStrategy;
    private Estimator realBestEstimator; // the best estimator on available samplers
    private Estimator realWorstEstimator;
    private Estimator onlineOptimalEstimator;
    private Estimator offlineOptimalEstimator; // the best estimator on all samplers.
    private Estimator twoStepChosenSampler;
    private boolean recordIterationState = false;
    private boolean storeValueWeights = false;

    private ArrayList<ValueRank> precomputedRanks = new ArrayList<ValueRank>();
    private double qCnt;

    public CEstimator() {
        super();
    }

    public CEstimator(LinkedHashMap<String, Estimator> baseSamplers, long randseed) {
        this.allSamplers = baseSamplers;
        this.randseed = randseed;
        this.rand = new Random(randseed);
//        findAndSetRealBestEstimator();
        //       findAndSetRealWorstEstimator();
    }


    public long getRandseed() {
        return this.randseed;
    }


    public void setPrecomputedRanks(ArrayList<ValueRank> precomputedRanks) {
        this.precomputedRanks = precomputedRanks;
    }

    public void setqCnt(double qCnt) {
        this.qCnt = qCnt;
    }

    public void setRandseed(long randseed) {
        this.randseed = randseed;
        this.rand = new Random(this.randseed);
    }

    public void setRecordIterationState(boolean isRecord) {
        this.recordIterationState = isRecord;
    }

    public void reset() throws SQLException {
        this.timer.reset();
        this.rand = new Random(this.randseed);
        this.allocationStrategy = new Strategy();
        this.iterationRecorder = new Dataframe();
        for (var v : allSamplers.values()) {
            v.reset();
        }
        queryResultEst = -1;
        estimatedVarOfCsampler = -1;
        queryCiEst = -1;
        samplerWeight = new LinkedHashMap<String, Double>();

        realBestEstimator = new Estimator(); // the best estimator on available samplers
        realWorstEstimator = new Estimator();
        onlineOptimalEstimator = new Estimator();
        offlineOptimalEstimator = new Estimator(); // the best estimator on all samplers.
        twoStepChosenSampler = new Estimator();
        precomputedRanks = new ArrayList<>();
        qCnt = 0.0;

        // idealVarzBound = Double.NaN;
        // realVarzBound = Double.NaN;
    }


    public String getRealWorstEstimator() {
        return this.realWorstEstimator.samplerName;
    }

    public String getRealBestEstimator() {
        return this.realBestEstimator.samplerName;
    }

    public Estimator getTwoStepChosenSampler() {
        return this.twoStepChosenSampler;
    }

    private Estimator findRealWorstEstimator(Collection<String> samplerList) {
        double maxVarz = 0.0;
        Estimator worstSampler = null;
        boolean firstIter = true;
        for (var sname : samplerList) {
            if (firstIter) {
                maxVarz = allSamplers.get(sname).getRealVarz();
                worstSampler = allSamplers.get(sname);
                firstIter = false;
            } else {
                var curVarz = allSamplers.get(sname).getRealVarz();
                if (curVarz > maxVarz) {
                    maxVarz = curVarz;
                    worstSampler = allSamplers.get(sname);
                }
            }
        }
        return worstSampler;
    }

    private Estimator findRealBestEstimator(Collection<String> samplerList) {
        double minVarz = 0.0;
        Estimator bestSampler = null;
        boolean firstIter = true;
        for (var sname : samplerList) {
            if (firstIter) {
                minVarz = allSamplers.get(sname).getRealVarz();
                bestSampler = allSamplers.get(sname);
                firstIter = false;
            } else {
                var curVarz = allSamplers.get(sname).getRealVarz();
                if (curVarz < minVarz) {
                    minVarz = curVarz;
                    bestSampler = allSamplers.get(sname);
                }
            }
        }
        return bestSampler;
    }


    public void setQuery(Query query) {
        this.query = query;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public void setStoreValueWeights(boolean storeVlaueWeights) {
        this.storeValueWeights = storeVlaueWeights;
    }


//    @Deprecated // set to each sampeler.
//    public void setIdealVarzBound(double idealVarzBound) {
//        this.idealVarzBound = idealVarzBound;
//    }
//
//    @Deprecated // set to each sampler.
//    public void setRealVarzBound(double realVarzBound) {
//        this.realVarzBound = realVarzBound;
//    }


    public void setResultFolder(String resultFolder) {
        this.resultFolder = resultFolder;
    }

    public String getName() {
        return "csampler";
    }


    public Dataframe getIterationRecorder() {
        return iterationRecorder;
    }


    public CEstimator setStrategy(Strategy strategy) {
        this.allocationStrategy = strategy;
        return this;
    }

    public String getStrategy() {
        return this.allocationStrategy.name.toString();
    }

    public double getEstimation() {
        return this.queryResultEst;
    }

    public double getVariance() {
        return -1;
    }

    public double getCi() {
        return -1;
    }


    public double computeRealCi() {
        if (query.aggFunction.equals("max") || query.aggFunction.equals("median")) {
            return Double.NaN;
        }

        if (allocationStrategy.name == SingleSampler || allocationStrategy.name == OfflineOptimal || allocationStrategy.name == OnlineOptimal) {
            if (this.availableSamplers.size() > 1)
                throw new IllegalStateException("available sampler number should be 1 for the strategy:" + allocationStrategy.name);
            var usedSamplerName = this.availableSamplers.iterator().next();
            var singleSampler = this.allSamplers.get(usedSamplerName);
            double realVar = singleSampler.getRealVarz() / (double) singleSampler.sampleSize;
            double ci = 1.96 * Math.sqrt(realVar);
            return ci;
        }

        double vari = 0.0;
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            double wi = samplerWeight.get(sname);
            double ni = sampler.sampleSize;
            if (ni > 0.0) {
                vari += wi * wi * sampler.getRealVarz() / ni;
            }
        }

        double ci = 1.96 * Math.sqrt(vari);
        return ci;
    }

    public int getSampleSize() {
        int size = 0;
        for (var sname : availableSamplers) {
            size += allSamplers.get(sname).sampleSize;
        }
        return size;
    }


    private Estimator getSamplerWithMostBudget() {
        int mostBudget = -1;
        Estimator res = new Estimator();
        for (var sname : availableSamplers) {

            if (allSamplers.get(sname).sampleSize > mostBudget) {
                mostBudget = (int) allSamplers.get(sname).sampleSize;
                res = allSamplers.get(sname);
            }
        }
        return res;
    }

    private double getRealCiOfEqualAllocation(int budget) {
        double n = budget / (double) availableSamplers.size();
        double vari = 0.0;
        for (var sname : availableSamplers) {
            double wi = 1.0 / availableSamplers.size();
            vari += wi * wi * (allSamplers.get(sname).getRealVarz() / n);
        }
        return 1.96 * Math.sqrt(vari);
    }

    private void record(int iterId, Estimator chosenSampler) throws Exception {
        if (!recordIterationState) return;
        int budget = getSampleSize();
        if (allocationStrategy.containPar("recordIterState.budgetList")) {
            var budgetList = (HashSet<Integer>) allocationStrategy.getPar("recordIterState.budgetList");
            if (!budgetList.contains(budget)) return;
        }
        estimatedResultAndCi();
        //timer.getElapsedTime();
        double timeStamp = timer.getTotalTime();
        double realValue = this.allSamplers.values().iterator().next().getRealResult();
        double rlterror = Math.abs(getEstimation() - realValue) / realValue;
        double realCi = computeRealCi();
        double cierror = realCi / realValue;
        double ciOfBestSampler = 1.96 * Math.sqrt(realBestEstimator.getRealVarz() / budget);
        double bestSamplerCiError = ciOfBestSampler / realValue;
        double ciOfWorstSampler = 1.96 * Math.sqrt(realWorstEstimator.getRealVarz() / budget);
        double worstSamplerCiError = ciOfWorstSampler / realValue;
        double equalAllocCiError = getRealCiOfEqualAllocation(budget) / realValue;

        var row = new Row();
        row.put("strategy", allocationStrategy.name.toString());
        row.put("gid", query.groupId);
        row.put("qid", query.qid);
        row.put("uid", uid);
        row.put("sql", query.getSql());
        row.put("t", this.t);
        row.put("timeStamp", timeStamp);
        row.put("initSize", allocationStrategy.getOrElse("initSize", "None"));
        row.put("initSizeRate", allocationStrategy.getOrElse("initSizeRate", "None"));
        row.put("availableSamplerNum", getAvailableSamplerNum());
        row.put("availableSamplers", getSamplerNameState(availableSamplers));
        row.put("allSamplers", getSamplerNameState(allSamplers.keySet()));
        row.put("chosenSampler", chosenSampler.samplerName);
        row.put("weightAllocationStrategy", allocationStrategy.getOrElse("weightAllocationStrategy", "None"));
        // row.put("estimatorUid", uid);
        // row.put("iterId", (String.valueOf(iterId)));
        row.put("batchSize", allocationStrategy.getOrElse("batchSize", "None"));
        row.put("budget", budget);
        //  row.put("chosenSampler", chosenSampler.getName());

        row.put("worstSampler", realWorstEstimator.getName());
        row.put("bestSampler", realBestEstimator.getName());
        var offlineOptimalSamplerName = (allocationStrategy.name == OfflineOptimal) ? offlineOptimalEstimator.getName() : "None";
        row.put("offlineOptimalSampler", offlineOptimalSamplerName);

        var onlineOptimalSamplerName = (allocationStrategy.name == OnlineOptimal) ? onlineOptimalEstimator.getName() : "None";
        row.put("onlineOptimalSampler", onlineOptimalSamplerName);

        row.put("samplerWithMostBudget", getSamplerWithMostBudget().samplerName);

        if (allocationStrategy.name == StrategyName.IdealLcb || allocationStrategy.name == StrategyName.Lcb || allocationStrategy.name == StrategyName.AdaptiveLcb) {
            row.put("idealVarzBoundState", getIdealVarzBoundState());
            row.put("realVarzBoundState", getRealVarzBoundState());
            row.put("adaptiveVarzBoundState", getAdaptiveVarzBoundState());
            row.put("lcbState", getCurrentLcbState());

            for (var sname : availableSamplers) {
                if (sname.equals(chosenSampler.samplerName)) {
                    row.put(sname + ".currBatchVarzEst", allSamplers.get(sname).currBatchVarzEst);
                } else {
                    row.put(sname + ".currBatchVarzEst", "NaN");
                }
            }
        }

        if (allocationStrategy.name == StrategyName.Eps) {
            row.put("decayConstant", allocationStrategy.getOrElse("decayConstant", "None"));
            row.put("currEps", this.currEps);
            row.put("epsCurrentSelection", this.epsCurrChosenApproach);
        }

        if (allocationStrategy.name == SingleSampler) {
            row.put("singleSamplerName", allocationStrategy.getOrElse("singleSamplerName", "None"));
        }

        row.put("realValue", realValue);
        row.put("estimation", getEstimation());
        row.put("ciEstimation", getQueryCiEst());
        row.put("realCi", computeRealCi());
        row.put("weightState", getWeightState());
        row.put("sizeState", getSampleSizeAllocationState());
        row.put("realVarzState", getRealVarzState());
        row.put("estimatedVarzState", getEstimatedVarzState());
        row.put("selRowsState", getSelRowsState());
        row.put("rltError", rlterror);
        row.put("ciError", cierror);
        row.put("equalAllocCiError", equalAllocCiError);
        row.put("bestSamplerCiError", bestSamplerCiError);
        row.put("worstSamplerCiError", worstSamplerCiError);


        if (query.aggFunction.equals("max") || query.aggFunction.equals("median")) {
            var realValueRanks = Experiment.getRanks(realValue, precomputedRanks, qCnt);
            var estimationRank = Experiment.getRanks(getEstimation(), precomputedRanks, qCnt);
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

        iterationRecorder.insert(row);
    }


    private void singleSamplerAllocation(int budget) throws Exception {

        Estimator singleSampler;

        switch (allocationStrategy.name) {
            case SingleSampler: {
                if (!allocationStrategy.containPar("singleSamplerName"))
                    throw new IllegalArgumentException("parameter does not find: singleSamplerName");
                var singleSamplerName = (String) allocationStrategy.getPar("singleSamplerName");
                singleSampler = this.allSamplers.get(singleSamplerName);
                break;
            }

            case OfflineOptimal: {
                singleSampler = offlineOptimalEstimator;
                break;
            }

            case OnlineOptimal: {
                singleSampler = onlineOptimalEstimator;
                break;
            }

            default:
                throw new IllegalStateException("unhandled alloc strategy:" + allocationStrategy.name);
        }

        checkBatchGrowStrategy();
        int leftBudget = budget;
        int iterId = 0;
        while (leftBudget > 0) {
            int tupleNum = getAllocatedTuples(singleSampler, leftBudget);
            int actualFetchTuples = singleSampler.addSample(tupleNum);
            leftBudget -= actualFetchTuples;
            record(iterId, singleSampler);
        }
    }


    private int initAllocation(int totalBudget) throws SQLException {
        if (!allocationStrategy.containPar("initSize"))
            throw new IllegalArgumentException("parameter does not find: initSize");

        int initBudget = (int) allocationStrategy.getPar("initSize");

        if (initBudget < availableSamplers.size())
            throw new IllegalArgumentException(" the initBudget should be larger than number of allSamplers, current initBudget is " + initBudget);

        int tupleNumPerSampler = initBudget / availableSamplers.size();
        for (var sname : availableSamplers) {
            allSamplers.get(sname).addSample(tupleNumPerSampler);
        }

        int leftBudget = totalBudget - tupleNumPerSampler * availableSamplers.size();
        Logger.logger.info("strategyName:" + allocationStrategy.name + ", initSize:" + initBudget + ", sampler size:" + allSamplers.size());
        return leftBudget;
    }


    public void query(int budget) throws Exception {

        if ((int) allocationStrategy.getPar("totalBudget") != budget)
            throw new IllegalArgumentException("totalBudget in strategy not equal to budget in query");
        if (!allocationStrategy.containPar("recordIterState"))
            throw new IllegalArgumentException("parameter recordIterState is not find.");


        this.onlineSamplers = new LinkedHashSet<>();
        if (allocationStrategy.containPar("availableSamplers")) {
            var snames = (List<String>) allocationStrategy.getPar("availableSamplers");
            this.availableSamplers = new LinkedHashSet<>(snames);
        } else {
            this.availableSamplers = new LinkedHashSet<>();
            switch (allocationStrategy.name) {
                case SingleSampler: {
                    this.availableSamplers.add((String) allocationStrategy.getPar("singleSamplerName"));
                    break;
                }

                case OfflineOptimal: {
                    this.offlineOptimalEstimator = findRealBestEstimator(allSamplers.keySet());
                    this.availableSamplers.add(this.offlineOptimalEstimator.samplerName);
                    break;
                    // return; // do not do the actual query, compute real variance outside.
                }

                // online optimal and offline optimal will have different allSamplers.
                case OnlineOptimal: {
                    var snames = (List<String>) allocationStrategy.getPar("onlineSamplers");
                    this.onlineSamplers = new LinkedHashSet<>(snames);
                    this.onlineOptimalEstimator = findRealBestEstimator(this.onlineSamplers);
                    this.availableSamplers.add(this.onlineOptimalEstimator.samplerName);
                    break;
                    //  return; // do not do the actual query, compute real variance outside.
                }

                default: {
                    this.availableSamplers.addAll(allSamplers.keySet());
                    break;
                }
            }
        }

        for (var s : allSamplers.values()) {
            s.setStoreValueWeights(this.storeValueWeights);
        }


        realBestEstimator = findRealBestEstimator(this.availableSamplers);
        realWorstEstimator = findRealWorstEstimator(this.availableSamplers);


        this.recordIterationState = (boolean) allocationStrategy.getPar("recordIterState");

        if (allocationStrategy.name.equals(OfflineOptimal) || allocationStrategy.name.equals(OnlineOptimal)) {
            return; // do not do the actual query, compute real variance outside.
        }


        timer.reset();
        switch (allocationStrategy.name) {
            case Lcb: {
                lcbAllocation(budget);
                break;
            }
            case IdealLcb: {
                lcbAllocation(budget);
                break;
            }
            case AdaptiveLcb: {
                lcbAllocation(budget);
                break;
            }

            case Eps: {
                epsAllocation(budget);
                break;
            }
            case TwoStepComb: {
                twoStepAllocation(budget);
                break;
            }

            case BlinkDB: {
                // BlinkDB is the same as two step allocation,
                // the difference is that it only uses the best sampler and do not combine other samplers.
                twoStepAllocation(budget);
                break;
            }

            case Random: {
                randomAllocation(budget);
                break;
            }
            case Equal: {
                equalAllocation(budget);
                break;
            }

            case Given: {
                givenAllocation();
                break;
            }

            case SingleSampler: {
                singleSamplerAllocation(budget);
                break;
            }

            case OfflineOptimal: {
                // it will be distinguished from singleSampler strategy based on strategy.name
                singleSamplerAllocation(budget);
                break;
            }

            case OnlineOptimal: {
                singleSamplerAllocation(budget);
                break;
            }

            default:
                throw new Exception("unknown strategy:" + allocationStrategy.name);
        }

        estimatedResultAndCi();
        //  computeResult();
    }


    // this function mainly is used for debuging.
    private void givenAllocation() throws IOException, SQLException {

        Logger.logger.warn("running given allocation strategy, the input budget will be ignored.");

        if (!allocationStrategy.containPar("fixed.allocationMap"))
            throw new IllegalArgumentException("parameter does not find: fixed.allocationMap");

        var allocMap = (HashMap<String, Integer>) allocationStrategy.getPar("fixed.allocationMap");
        for (var item : allocMap.entrySet()) {
            var sname = item.getKey();
            var size = item.getValue();
            var sampler = this.allSamplers.get(sname);
            if (size < 0) throw new IllegalStateException("size cannot be negative! samplerName=" + sname);

            int actualFetchRows = sampler.addSample(size);

            if (actualFetchRows < size)
                Logger.logger.warn("sampler:{} requires to fetch {} rows, but actually fetch {} rows", sname, size, actualFetchRows);
        }
    }


    private void twoStepAllocation(int budget) throws IOException, SQLException {

        int leftBudget = initAllocation(budget);

        if (leftBudget < 0)
            throw new IllegalStateException("leftBudget should >= 0, current leftBudget = " + leftBudget);

        Logger.logger.info("running two step allocation");
        var samplerList = new ArrayList<Estimator>();
        for (var sname : availableSamplers) {
            samplerList.add(allSamplers.get(sname));
        }

        var bestSampler = findBestSampler(samplerList);
        this.twoStepChosenSampler = bestSampler;
        if (leftBudget == 0) return;
        if (leftBudget > 0) bestSampler.addSample(leftBudget);

    }


    private void estimatedResultAndCi() {
        if (allocationStrategy.name == SingleSampler || allocationStrategy.name == OfflineOptimal || allocationStrategy.name == OnlineOptimal) {
            if (this.availableSamplers.size() > 1) {
                throw new IllegalStateException("the available sampler number should be 1 for strategy: " + allocationStrategy.name + ", current number is " + availableSamplers.size());
            }
            String usedSamplerName = this.availableSamplers.iterator().next();
            Estimator usedSampler = this.allSamplers.get(usedSamplerName);
            this.queryResultEst = usedSampler.getEstimation();
            this.estimatedVarOfCsampler = usedSampler.getVariance();
            this.queryCiEst = 1.96 * Math.sqrt(estimatedVarOfCsampler);
            return;
        }

        // blinkdb is similar to two-step but does not combine samplers.
        if (allocationStrategy.name == BlinkDB) {
            var singleSampler = twoStepChosenSampler;
            this.queryResultEst = singleSampler.getEstimation();
            this.estimatedVarOfCsampler = singleSampler.getVariance();
            this.queryCiEst = 1.96 * Math.sqrt(estimatedVarOfCsampler);
            for (var sname : availableSamplers) {
                if (sname.equals(twoStepChosenSampler.samplerName)) {
                    samplerWeight.put(sname, 1.0);
                } else {
                    samplerWeight.put(sname, 0.0);
                }
            }
            return;
        }


        String weightStrategy = (String) allocationStrategy.getOrElse("weightAllocationStrategy", "pseudoOptimal");


        var weightedValues = new ArrayList<ValueWeight>();  // for median query.

        double maxValue = 0.0;
        // find the sampler with maximal size, used for mostSampleOnly weight allocation.
        long maxSampleSize = 0;
        String samplerWithMaxSize = null;
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            weightedValues.addAll(sampler.valueWeights);

            if (sampler.sampleSize > maxSampleSize) {
                maxSampleSize = sampler.sampleSize;
                samplerWithMaxSize = sampler.samplerName;
            }

            // maxValue is used to answer max query.
            if (sampler.currentMaxValue > maxValue) {
                maxValue = sampler.currentMaxValue;
            }
        }

        if (query.aggFunction.equals("max")) {
            this.queryResultEst = maxValue;
            return;
        }


        if (query.aggFunction.equals("median")) {
            Collections.sort(weightedValues);
            double medianEstimation = 0.0;
            double totalWeight = 0.0;
            for (var wv : weightedValues) {
                totalWeight += wv.weight;
            }

            double medianWeightSum = totalWeight / 2.0;
            double currWeightSum = 0.0;
            for (var wv : weightedValues) {
                currWeightSum += wv.weight;
                if (currWeightSum >= medianWeightSum) {
                    medianEstimation = wv.value;
                    break;
                }
            }

            this.queryResultEst = medianEstimation;
            return;
        }


        double varNumerator = 0;
        double estNumerator = 0;
        double totalw = 0;
        double totalSampleSize = 0;
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            //Logger.log(sampler.tupleNum);
            // double est = sampler.getEstimation();
            //this is used for single sampler approach.
            double wi = 0.0;
            double var = sampler.getVariance();
            totalSampleSize += sampler.sampleSize;

            switch (weightStrategy) {
                case "pseudoOptimal": {
                    if (sampler.selRows > 0) {
                        wi = (var > 0.0) ? 1.0 / var : 1.0;
                    } else wi = 0.0;
                    break;
                }

                case "realOptimalAlloc": {
                    double realVari = sampler.getRealVarz() / sampler.sampleSize;
                    wi = (realVari > 0.0) ? 1.0 / realVari : 1.0;
                    break;
                }

                case "mostSampleOnly": {
                    wi = (sname.equals(samplerWithMaxSize)) ? 1.0 : 0.0;
                    break;
                }

                case "propToSize": {
                    wi = sampler.sampleSize;
                    break;
                }

                case "propToSelectivity": {
                    wi = (double) (sampler.selRows) / (double) (sampler.sampleSize);
                    break;
                }

                case "propToSelRows": {
                    wi = sampler.selRows;
                    break;
                }

                default:
                    throw new IllegalArgumentException("unknown weight allocation strategy:" + weightStrategy);
            }
            //Logger.log(sampler.tupleNum, est, var);
            totalw += wi;
            estNumerator += wi * sampler.getEstimation();
            varNumerator += wi * wi * var;

            samplerWeight.put(sampler.samplerName, wi);
        }


        if (totalw > 0.0) {
            for (var sname : availableSamplers) {
                samplerWeight.put(sname, samplerWeight.get(sname) / totalw);
            }
            this.queryResultEst = estNumerator / totalw;
            this.estimatedVarOfCsampler = varNumerator / (totalw * totalw);
            this.queryCiEst = 1.96 * Math.sqrt(estimatedVarOfCsampler);
        } else {
            double estNumer = 0.0;
            double varNume = 0.0;

            for (var sname : availableSamplers) {
                var sampler = allSamplers.get(sname);
                double wi = sampler.sampleSize / totalSampleSize;
                samplerWeight.put(sampler.samplerName, wi);
                if (sampler.sampleSize > 0) {
                    estNumer += wi * sampler.getEstimation();
                    varNume += wi * wi * sampler.getVariance();
                }
            }

            this.queryResultEst = estNumer;
            this.estimatedVarOfCsampler = varNume;
            this.queryCiEst = 1.96 * Math.sqrt(estimatedVarOfCsampler);
        }

        return;
    }


    private String mapToJson(Map<String, String> map) {
        var sj = new StringJoiner(",");
        for (var kv : map.entrySet()) {
            var state = String.format("'%s':%s", kv.getKey(), kv.getValue());
            sj.add(state);
        }
        var json = String.format("\"{%s}\"", sj.toString());
        return json;
    }


    public int getAvailableSamplerNum() {
        return this.availableSamplers.size();
    }


    public String getOfflineOptimalSampler() {
        if (this.allocationStrategy.name == OfflineOptimal)
            return this.offlineOptimalEstimator.samplerName;
        return "None";
    }

    public String getOnlineOptimalSampler() {
        if (this.allocationStrategy.name == OnlineOptimal)
            return this.onlineOptimalEstimator.samplerName;
        return "None";
    }


    public String getAvailableSamplerNameState() {
        return getSamplerNameState(this.availableSamplers);
    }

    public String getOnlineSamplerNameState() {
        return getSamplerNameState(this.onlineSamplers);
    }

    public String getAllSamplerNameState() {
        return getSamplerNameState(this.allSamplers.keySet());
    }

    public String getSamplerNameState(Collection<String> samplers) {
        var sj = new StringJoiner(",");
        for (var k : samplers) {
            //if there is just one sampler, directly return its name. This will be used for exp of computing real varz for single sampler.
            if (samplers.size() == 1) return k;
            sj.add(k);
        }
        var res = String.format("\"{%s}\"", sj.toString());
        return res;
    }


    public String getSampleSizeAllocationState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            map.put(sampler.samplerName, String.valueOf(sampler.sampleSize));
        }
        return mapToJson(map);
    }


    public String getSelRowsState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            map.put(sampler.samplerName, String.valueOf(sampler.selRows));
        }
        return mapToJson(map);
    }

    public String getAdaptiveVarzBoundState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            map.put(sampler.samplerName, String.valueOf(sampler.adaptiveVarzBound));
        }
        return mapToJson(map);
    }


    public String getRealVarzState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            String value = String.valueOf(sampler.getRealVarz());
            // if there is just one sampler, directly return its value. This will be used for getting real varz for single sampler.
            if (availableSamplers.size() == 1) return value;
            map.put(sampler.samplerName, value);
        }
        return mapToJson(map);
    }


    public String getIdealVarzBoundState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            map.put(sampler.samplerName, String.valueOf(sampler.idealVarzBound));
        }
        return mapToJson(map);
    }


    public String getRealVarzBoundState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            map.put(sampler.samplerName, String.valueOf(sampler.realVarzBound));
        }
        return mapToJson(map);
    }


    public String getCurrentLcbState() throws Exception {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            double lcbWeight = (double) allocationStrategy.getPar("lcbWeight");
            String lcbStrategy = (String) allocationStrategy.getPar("boundType");
            double lcb = sampler.computeLcbOfVarz(lcbWeight, lcbStrategy, this.t);
            map.put(sampler.samplerName, String.valueOf(lcb));
        }
        return mapToJson(map);
    }

    public String getWeightState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            map.put(sampler.samplerName, samplerWeight.getOrDefault(sampler.samplerName, Double.NaN).toString());
        }
        return mapToJson(map);
    }

    public String getEstimatedVarzState() {
        var map = new HashMap<String, String>();
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            map.put(sampler.samplerName, String.valueOf(sampler.currentEstimatedVarz()));
        }
        return mapToJson(map);
    }


    public double getQueryCiEst() {
        return this.queryCiEst;
    }




    /*
    private  void initAllocation(int tupleNum) throws IOException {
        int tupleNumPerSampler = tupleNum / allSamplers.size();
        for (var sampler : allSamplers.values()) {
            sampler.addSample(tupleNumPerSampler);
        }
    }
*/


    class EstimatorComparator implements Comparator<Estimator> {
        public int compare(Estimator a, Estimator b) {
            double avarz = a.currentEstimatedVarz();
            double bvarz = b.currentEstimatedVarz();

            if (allocationStrategy.name == StrategyName.Eps || allocationStrategy.name == StrategyName.TwoStepComb || allocationStrategy.name == BlinkDB) {
                if (a.selRows == 0 || b.selRows == 0) return (int) (b.selRows - a.selRows);
                if (query.aggFunction.equals("max")) return Double.compare(-a.currentMaxValue, -b.currentMaxValue);
                double asel = a.selRows / (double) a.sampleSize;
                double bsel = b.selRows / (double) b.sampleSize;
                if (query.aggFunction.equals("median")) return Double.compare(-asel, -bsel);
                return Double.compare(avarz, bvarz);
            }

            if (allocationStrategy.name == StrategyName.Lcb || allocationStrategy.name == StrategyName.IdealLcb || allocationStrategy.name == StrategyName.AdaptiveLcb) {

                double lcbWeight = (double) allocationStrategy.getPar("lcbWeight");
                String lcbStrategy = (String) allocationStrategy.getPar("boundType");
                double alcb = 0;
                double blcb = 0;
                try {
                    alcb = a.computeLcbOfVarz(lcbWeight, lcbStrategy, t);
                    blcb = b.computeLcbOfVarz(lcbWeight, lcbStrategy, t);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                if (Double.compare(alcb, blcb) == 0) return Double.compare(avarz, bvarz);
                return Double.compare(alcb, blcb);
            }

            throw new IllegalStateException("unhandled strategy for sampler comparision, strategy name:" + allocationStrategy.name.toString());
        }
    }


    /*
    private int getMaxAllocationTimes()
    {
        int maxAlloc = 0;
        for (var s: allSamplers.values())
        {
            if (s.allocateTimes > maxAlloc)
            {
                maxAlloc = s.allocateTimes;
            }
        }
        return maxAlloc;
    }
*/

    /*
    private void setT()
    {
        double t = Math.max(2.0, getMaxAllocationTimes());
        for (var sampler : allSamplers.values())
        {
            sampler.t = t;
        }
    }
*/


    private Estimator findBestSampler(ArrayList<Estimator> samList) {
        var samplerList = new ArrayList<Estimator>(samList);
        for (var s : samplerList) {
            if (s.cursorEnd) samplerList.remove(s);
        }
        if (samplerList.isEmpty())
            throw new IllegalStateException("input samplerLisr is empty, or all sampler has fetched all tuples");

        var samplerComparator = new EstimatorComparator();
        Collections.sort(samplerList, samplerComparator);
        int pos = 1;
        for (int i = 1; i < samplerList.size(); i++) {
            if (samplerComparator.compare(samplerList.get(0), samplerList.get(i)) == 0) {
                pos = i + 1;
            }
        }
        int chosenPos = this.rand.nextInt(pos);
        return samplerList.get(chosenPos);
    }


    private void randomAllocation(int budget) throws Exception {
        checkBatchGrowStrategy();
        int leftBudget = initAllocation(budget);

        int samplerNum = availableSamplers.size();
        var samplerList = new ArrayList<Estimator>();
        for (var sname : availableSamplers) {
            samplerList.add(allSamplers.get(sname));
        }

        int iterId = 0;
        while (leftBudget > 0) {
            iterId += 1;
            var chosenSampler = samplerList.get(this.rand.nextInt(samplerNum));
            int tupleNum = getAllocatedTuples(chosenSampler, leftBudget);
            int actualFetchTuples = chosenSampler.addSample(tupleNum);
            leftBudget -= actualFetchTuples;
            record(iterId, chosenSampler);
        }

    }

    private void equalAllocation(int budget) {
        int samplerNum = availableSamplers.size();
        int budgetPerSampler = budget / samplerNum;
        int usedBudget = 0;
        for (var sname : availableSamplers) {
            var sampler = allSamplers.get(sname);
            var actualFetchTuples = sampler.addSample(budgetPerSampler);
            usedBudget += actualFetchTuples;
        }

        var leftBudget = budget - usedBudget;
        if (leftBudget > 0) {
            var samplerList = new ArrayList<Estimator>();
            for (var sname : availableSamplers) {
                samplerList.add(allSamplers.get(sname));
            }

            var randSampler = samplerList.get(this.rand.nextInt(samplerNum));
            randSampler.addSample(leftBudget);
        }
    }

    private void epsAllocation(int budget) throws Exception {
        checkBatchGrowStrategy();
        int leftBudget = initAllocation(budget);

        int samplerNum = availableSamplers.size();
        var samplerList = new ArrayList<Estimator>();
        for (var sname : availableSamplers) {
            samplerList.add(allSamplers.get(sname));
        }

        int iterId = 0;
        while (leftBudget > 0) {
            iterId += 1;
            double decayConstant = Double.valueOf(allocationStrategy.getPar("decayConstant").toString());
            double epsilon = Math.min(1.0, decayConstant / (double) iterId);

            this.currEps = epsilon;

            /*
            if (epsilonStrategy.equals("greedy"))
            {
                epsilon = (double) allocationStrategy.getPar("epsilon.greedy.rate");

            }
            else if (epsilonStrategy.equals("decay"))
            {
                double decayConstant = (double) allocationStrategy.getPar("epsilon.decay.constant");
                epsilon = Math.min(1.0, decayConstant/(double) iterId);
           }
            else throw new Exception("unknown epsilonStrategy:" + epsilonStrategy);
            */


            // currentEplison = epsilon;
            //record(iterId);
            var bestSampler = findBestSampler(samplerList);
            var randSampler = samplerList.get(this.rand.nextInt(samplerNum));

            //with prob. 1 - eps choose best sampler, prob. eps choose randSampler.
            boolean chosenBest = (this.rand.nextDouble() > epsilon);
            var chosenSampler = (chosenBest) ? bestSampler : randSampler;

            this.epsCurrChosenApproach = (chosenBest) ? "empiricallyBest" : "random";


            int tupleNum = getAllocatedTuples(chosenSampler, leftBudget);

            //int tuples = (int) Math.min(leftBudget, chosenSampler.tupleNum);
            int actualFetchTuples = chosenSampler.addSample(tupleNum);
            leftBudget -= actualFetchTuples;
            record(iterId, chosenSampler);
        }
    }


    private void checkBatchGrowStrategy() throws Exception {
        assert allocationStrategy.containPar("batchType") : "does not contains parameter bathType";
        String batchGrowStrategy = (String) allocationStrategy.getPar("batchType");
        switch (batchGrowStrategy) {
            case "constant":
                assert allocationStrategy.containPar("batchSize");
                break;
            case "exponential":
                assert allocationStrategy.containPar("batchIncreaseRate");
                break;
            default:
                throw new Exception("unknown batchType: " + batchGrowStrategy);
        }
    }


    private int getAllocatedTuples(Estimator chosenEstimator, int leftBudget) throws Exception {
        String batchGrowStrategy = (String) allocationStrategy.getPar("batchType");
        if (batchGrowStrategy.equals("constant")) {
            // tupleNum = (int) allocationStrategy.getPar("batchGrow.constant.size");
            return Math.min(
                    (int) allocationStrategy.getPar("batchSize"),
                    leftBudget);
        } else if (batchGrowStrategy.equals("exponential")) {
            double rate = (double) allocationStrategy.getPar("batchIncreaseRate");
            return Math.min(
                    (int) ((rate - 1.0) * chosenEstimator.sampleSize),
                    leftBudget);
        } else throw new Exception("unknow batchType:" + batchGrowStrategy);
    }


    private void lcbAllocation(int budget) throws Exception {

        checkBatchGrowStrategy();
        if (!allocationStrategy.containPar("lcbWeight"))
            throw new IllegalArgumentException("does not find parameter: lcbWeight");
        if (!allocationStrategy.containPar("boundType"))
            throw new IllegalArgumentException("does not find parameter: bountType (either hoeffding or cheby)");
        if (!allocationStrategy.containPar("batchSize"))
            throw new IllegalArgumentException("does not find parameter: batchSize");


        // int lcbChunkSize = (int) allocationStrategy.getPar("batchSize");
        Logger.logger.info("availableSampler:{}", getSamplerNameState(this.availableSamplers));
        for (var sname : availableSamplers) {
            var s = allSamplers.get(sname);
            switch (allocationStrategy.name) {
                case IdealLcb: {
                    int totalBudget = (int) allocationStrategy.getPar("totalBudget");
                    int batchSize = (int) allocationStrategy.getPar("batchSize");
                    s.setVarzBoundType("ideal");
                    s.computeAndSetIdealVarzBound(totalBudget, batchSize);
                    break;
                }
                case Lcb: {
                    s.setVarzBoundType("real");
                    s.setRealVarzBound();
                    break;
                }
                case AdaptiveLcb: {
                    Logger.logger.info("samplerName:{}", s.samplerName);
                    s.setVarzBoundType("adaptive");
                    s.setRealVarzBound(); //requires realVarzBound as init. when no tuples is satifified.
                    break;
                }
            }
        }


        int leftBudget = initAllocation(budget);

        var samplerList = new ArrayList<Estimator>();
        for (var sname : availableSamplers) {
            samplerList.add(allSamplers.get(sname));
        }

        int iterId = 0;


        while (leftBudget > 0) {
            iterId += 1;

            this.t = iterId + 1;
            //setT();
            // record(iterId);
            var chosenSampler = findBestSampler(samplerList);
            int tupleNum = getAllocatedTuples(chosenSampler, leftBudget);
            int actualReadRows = chosenSampler.addSample(tupleNum);
            double est = chosenSampler.getEstimation();
            double ci = chosenSampler.getCi();
            //Logger.log(sname, "est:"+ est, "ci:" + ci, "["+(est-ci)+","+(est+ci)+"]");
            leftBudget -= actualReadRows;
            record(iterId, chosenSampler);
        }
    }
}