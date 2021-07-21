package com.samcomb.types;

import java.util.HashMap;


public class Query {

    public String qid;
    public String groupId;
    public String aggFunction;
    public String aggColumn;
    public String groupColumn;

    public HashMap<String, NamedPredicate<Object>> predicate;
    public String predSql;
    public String tableName;
    // public String sql;

    public Query() {
        super();
    }

    public Query(String qid, String aggColumn, HashMap<String, NamedPredicate<Object>> predicate) {
        this.qid = qid;
        this.aggColumn = aggColumn;
        this.predicate = predicate;
    }

    public Query(String qid, String aggFunction, String aggColumn, String predSql) {
        this.qid = qid;
        this.aggFunction = aggFunction;
        this.aggColumn = aggColumn;
        this.predSql = predSql;
        //this.tableName = tableName;
        // this.sql = String.format("SELECT %s(%s) as result FROM %s WHERE %s", aggFunction, aggColumn, tableName, predSql);
    }


    public String getSql() {
        if (this.aggFunction.equals("median")) {
            return String.format("SELECT percentile_disc(0.5) within group (order by %s) from %s WHERE %s", aggColumn, tableName, predSql);
        }
        return String.format("SELECT %s(%s) as result FROM %s WHERE %s", aggFunction, aggColumn, tableName, predSql);
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getAggcolumnAndPredIndicatorSql(String tableName) {
        return String.format("SELECT %s, CASE WHEN %s THEN true ELSE false END as predIndicator FROM %s", aggColumn, predSql, tableName);
    }

    @Override
    public String toString() {
        return String.format("aggCol:%s, pred:%s\n", aggColumn, predSql);
    }
}
