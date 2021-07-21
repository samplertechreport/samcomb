package com.samcomb.config.expconfig;

import com.samcomb.allocation.Strategy;
import com.samcomb.allocation.StrategyName;
import com.samcomb.config.Config;
import com.samcomb.config.DefaultExpConfig;
import com.samcomb.config.DefaultStrategyConfig;
import com.samcomb.types.AggregateType;

import java.util.*;

public class VarySamplerNumConfig {

    // note that the best order depends how sampler perform on the queries. This should get based on rankSingleSampler exp.
    // need to change once the queries changed.

    public final static List<String> bestSamplerOrder = Arrays.asList(
            "sf_l_linestatus",
            "bk_l_orderkey",
            "sf_l_returnflag",
            "sf_l_shipmode",
            "sf_l_shipinstruct",
            "bk_l_suppkey",
            "uf_None",
            "bk_l_tax",
            "sf_l_shipdate",
            "sf_l_receiptdate",
            "sf_l_commitdate",
            "bk_l_partkey",
            "bk_l_discount",
            "bk_l_linenumber",
            "bk_l_extendedprice",
            "bk_l_quantity");

    public static ArrayList<String> getWorstSamplerOrder() {
        var worstSamplerOrder = new ArrayList<String>();
        int totalSamplers = VarySamplerNumConfig.bestSamplerOrder.size();
        for (int i = 0; i < VarySamplerNumConfig.bestSamplerOrder.size(); i++) {
            worstSamplerOrder.add(VarySamplerNumConfig.bestSamplerOrder.get(totalSamplers - i - 1));
        }
        return worstSamplerOrder;
    }


    // public final static List<String> bestSamplerOrderColumns = Arrays.asList(
    //     "l_linestatus", 
    //     "l_orderkey", 
    //     "l_returnflag", 
    //     "l_shipmode", 
    //     "l_shipinstruct", 
    //     "l_suppkey",
    //     "None",
    //     "l_tax",
    //     "l_shipdate",
    //     "l_receiptdate",
    //     "l_commitdate",
    //     "l_partkey",
    //     "l_discount",
    //     "l_linenumber",
    //     "l_extendedprice",
    //     "l_quantity");

    // public final static List<String> bestSamplerOrderTypes = Arrays.asList(
    //     "sf",
    //     "bk",
    //     "sf",
    //     "sf",
    //     "sf",
    //     "bk",
    //     "uf",
    //     "bk",
    //     "sf",
    //     "sf",
    //     "sf",
    //     "bk",
    //     "bk",
    //     "bk",
    //     "bk",
    //     "bk");


    public static ArrayList<String> getOtherSamplerNameList(List<String> samplerTypes, List<String> samplerColumns) {

        var defaultSamplerColumns = DefaultExpConfig.defaultSamplers.get("skew.columns");
        var defaultSamplerTypes = DefaultExpConfig.defaultSamplers.get("skew.types");
        ArrayList<String> defaultSamplerNameList = new ArrayList<String>();
        HashSet<String> defaultSamplerNameSet = new HashSet<>();
        for (int i = 0; i < defaultSamplerColumns.size(); i += 1) {
            String sname = defaultSamplerTypes.get(i) + "_" + defaultSamplerColumns.get(i);
            defaultSamplerNameList.add(sname);
            defaultSamplerNameSet.add(sname);
        }

        var otherSamplerNames = new ArrayList<String>();
        for (int i = 0; i < samplerColumns.size(); i += 1) {
            String sname = samplerTypes.get(i) + "_" + samplerColumns.get(i);
            if (!defaultSamplerNameSet.contains(sname)) {
                otherSamplerNames.add(sname);
            }
        }
        return otherSamplerNames;
    }

    public static ArrayList<String> getDefaultSamplerNames() {
        var defaultSamplerColumns = DefaultExpConfig.defaultSamplers.get("skew.columns");
        var defaultSamplerTypes = DefaultExpConfig.defaultSamplers.get("skew.types");
        var defaultSamplerNames = new ArrayList<String>();
        for (int i = 0; i < defaultSamplerColumns.size(); i += 1) {
            String sname = defaultSamplerTypes.get(i) + "_" + defaultSamplerColumns.get(i);
            defaultSamplerNames.add(sname);
        }
        return defaultSamplerNames;
    }

    public static Config getExpConfig() {
        var config = new Config();
        config.add("schemaNames", Arrays.asList("skew_s1_z2"));
        for (var schema : config.getStringList("schemaNames")) {
            config.add(schema, new Config());
        }
        for (var schema : config.getStringList("schemaNames")) {
            config.getConfig(schema).add("samplerColumns", DefaultExpConfig.defaultAllSamplers.get(schema + ".columns"));
            config.getConfig(schema).add("samplerTypes", DefaultExpConfig.defaultAllSamplers.get(schema + ".types"));

            config.getConfig(schema).add("jdbc", DefaultExpConfig.defaultJdbc);
            config.getConfig(schema).add("usePrecomputeSample", DefaultExpConfig.defaultUsePrecomputeSample);
            config.getConfig(schema).add("queryPath", DefaultExpConfig.defaultQueryPaths.get(schema));
            config.getConfig(schema).add("populationTable", schema + ".population");
            config.getConfig(schema).add("queryAggregateType", AggregateType.SUM);

        }

        return config;
    }

    public static ArrayList<Strategy> getStrategies(Config expConfig) {
        var strategies = new ArrayList<Strategy>();
        var allSamplerTypes = expConfig.getStringList("samplerTypes");
        var allSamplerColumns = expConfig.getStringList("samplerColumns");
        var defaultSamplerNameList = getDefaultSamplerNames();
        var otherSamplerNameList = getOtherSamplerNameList(allSamplerTypes, allSamplerColumns);

        LinkedHashMap<String, List<String>> samplerOrders = new LinkedHashMap<String, List<String>>();
        // samplerOrders.put("bestSamplerOrder", VarySamplerNumConfig.bestSamplerOrder);
        // samplerOrders.put("worstSamplerOrder", getWorstSamplerOrder());
        for (int randid = 0; randid < 10; randid += 1) {
            ArrayList<Integer> sidx = new ArrayList<>();
            for (int i = 0; i < otherSamplerNameList.size(); i++) {
                sidx.add(i);
            }
            Collections.shuffle(sidx, new Random(randid));
            var randomSampler = new ArrayList<String>();
            for (String sname : defaultSamplerNameList) {
                randomSampler.add(sname);
            }
            for (int id : sidx) {
                randomSampler.add(otherSamplerNameList.get(id));
            }
            samplerOrders.put("randomOrder" + randid, randomSampler);
        }

        for (String samplerOrderName : samplerOrders.keySet()) {
            var samplerArray = new ArrayList<String>();
            List<String> currSamplerOrder = samplerOrders.get(samplerOrderName);
            for (String sname : currSamplerOrder) {
                samplerArray.add(sname);
                var availableSamplers = new ArrayList<>(samplerArray);

                var adaptiveLcb = DefaultStrategyConfig.getDefaultAdaptiveLcb().copy();
                var random = DefaultStrategyConfig.getDefaultRandomAlloc().copy();
                for (var s : Arrays.asList(adaptiveLcb, random)) {
                    if (s.name != StrategyName.Equal) { // equal alloc has no init and batch size param.
                        int batchSize = (int) s.getPar("batchSize");
                        s.addPar("initSize", batchSize * allSamplerColumns.size());
                    }
                    s.addPar("availableSamplers", availableSamplers);
                    s.addPar("samplerNum", availableSamplers.size());
                    s.addPar("comments", samplerOrderName);
                    strategies.add(s);
                }
            }
        }

        return strategies;
    }

}
