package com.samcomb.experiment.exp;

import com.samcomb.config.ExpConfigManager;
import com.samcomb.config.expconfig.VaryDataPrecomputeTableConfig;
import com.samcomb.utils.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedHashSet;

public class CodeRunner {

    public static void run() throws Exception {

        String log = "\n---------------------------------\n " +
                "Please input the id of run code in one line, e.g., 1,2. The description of the id and code is listed as follows:\n" +
                "0: pre-create sample table for samplers\n" +
                "1: create index \n" +
                "2: generate queries \n" +
                "3: compare offline and online optimal sampler\n" +
                "4: compare two step combine and blinkdb\n" +
                "5: compare median query\n" +
                "6: compute max query\n" +
                "7: vary sampler number\n" +
                "8: varying c for eps-greedy\n" +
                "9: compare lcb, adaptiveLcb and eps\n" +
                "10: justify sampler combination\n" +
                "11: selectivity estimation\n" +
                "12: vary sample size\n" +
                "13: precompute tables for varying data\n" +
                "14: vary data size\n" +
                "15: rank single sampler (used for order of vary sampler number)\n";

        Logger.logger.info(log);

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        var strIds = br.readLine().split(",");
        var idSet = new LinkedHashSet<Integer>();
        for (var token : strIds) {
            var sid = token.strip().trim();
            idSet.add(Integer.valueOf(sid));
        }
        for (var id : idSet) {
            Logger.logger.info(id.toString());


            if (id == 0) CreateSamplerTables.run(ExpConfigManager.getExpConfig(ExpName.buildSamplers));
            if (id == 1) CreateIndex.run(ExpName.createIndex);
            if (id == 2) QueryGenerator.run(ExpName.generateQuery);
            if (id == 3) Experiment.run(ExpName.compareOfflineOptimalAndOnlineOptimalSampler);
            if (id == 4) Experiment.run(ExpName.compareTwoStepCombAndBlinkDb);
            if (id == 5) Experiment.run(ExpName.computeMedianQuery);
            if (id == 6) Experiment.run(ExpName.computeMaxQuery);
            if (id == 7) Experiment.run(ExpName.varySampelerNum);
            if (id == 8) Experiment.run(ExpName.varyC);
            if (id == 9) Experiment.run(ExpName.compareLcbEps);
            if (id == 10) Experiment.run(ExpName.combineVsNotcombine);
            if (id == 11) Experiment.run(ExpName.selectivityEstimation);
            if (id == 12) Experiment.run(ExpName.varySampleSize);
            if (id == 13) VaryDataExpPreprocessor.run(VaryDataPrecomputeTableConfig.getExpConfig());
            if (id == 14)
                Experiment.run(ExpName.varyDataSize);
            if (id == 15) Experiment.run(ExpName.rankSingleSampler);
        }
    }
}
