package com.samcomb.sampler;

public class OnlineFetcher {

    void reset() {
    }


    /*
    private void openCursor() throws SQLException {
        cursorEnd = false;
        this.fetchStat = conn.createStatement();

        Logger.logger.debug("open cursor, cursorEnd:{}, samplerName:{}", cursorEnd, samplerName);

        this.fetchStat.execute("BEGIN;");
        // Logger.logger.debug(samplerName);
        var samplerTableName = schemaName + "." + samplerName +"_data";
        this.defineCursorSql = String.format("DECLARE cur%s CURSOR FOR SELECT %s as data, CASE WHEN (%s) THEN 1 ELSE 0 END as pred, prob FROM %s ORDER BY randcol;", samplerName, query.aggColumn, query.predSql,samplerTableName);
        this.fetchStat.execute(defineCursorSql);
        Logger.logger.debug(defineCursorSql);
    }


    private void closeCursor() throws SQLException {
        Logger.logger.debug("close cursor, cursorEnd:{}, samplerName:{}", cursorEnd, samplerName);

        this.fetchStat.execute(String.format("CLOSE cur%s;", samplerName));
        this.fetchStat.execute("COMMIT");
        this.fetchStat.close();

    }

     */

    void getNext(int size) {


    }
}
