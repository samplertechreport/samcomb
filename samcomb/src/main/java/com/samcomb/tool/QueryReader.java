package com.samcomb.tool;

import com.samcomb.dataframe.Dataframe;
import com.samcomb.types.AggregateType;
import com.samcomb.types.Query;
import com.samcomb.utils.Logger;

import java.io.IOException;
import java.util.ArrayList;

public class QueryReader {

    public static ArrayList<Query> readPredAndGetQueries(String predPath, AggregateType queryAggregateType) throws IOException {
        var queries = new ArrayList<Query>();
        var predDf = Dataframe.readCsv(predPath);
        predDf.show();
        //var randomAggColumns = new ArrayList<String>(aggColumns);
        //Collections.shuffle(randomAggColumns, Rander.rand);

        int n = predDf.getRowNum();
        for (int i = 0; i < n; i++) {
            var pred = (String) predDf.getRow(i).get("pred");
            var aggCol = (String) predDf.getRow(i).get("aggCol");
            var aggFunc = (String) predDf.getRow(i).get("aggFunc");
            var groupId = (String) predDf.getRow(i).get("groupId");
            if (queryAggregateType == AggregateType.COUNT) {
                aggCol = "1.00";
                aggFunc = "sum";
            } else if (queryAggregateType == AggregateType.MAX) {
                aggFunc = "max";
            } else if (queryAggregateType == AggregateType.MEDIAN) {
                aggFunc = "median";
            }

            var query = new Query(String.valueOf(i), aggFunc, aggCol, pred);


            query.setGroupId(groupId);
            queries.add(query);
            Logger.logger.info(query.toString());
        }

        return queries;
    }


}
