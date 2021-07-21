package com.samcomb.dataframe;

import com.samcomb.utils.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

public class Row extends LinkedHashMap<String, Object> {

    public Row() {
        super();
    }

    public int getColumnCount() {
        return this.size();
    }

    public Set<String> getColumnNames() {
        return this.keySet();
    }

    /*
    public void add(String col, Object data){
        this.put(col, data);
    }
    */


    @Override
    public String toString() {
        var keyList = new ArrayList<String>(this.keySet());
        return toString(keyList, ", ");
    }

    public void show() throws IOException {
        var keyList = new ArrayList<String>(this.keySet());
        var header = String.join(", ", keyList);
        var valueStr = toString(keyList, ", ");
        Logger.logger.info("\n" + header + "\n" + valueStr);
    }

    public String toString(List<String> columns, String delimiter) {
        //   System.err.println("columns:" + columns.toString());
        var valueList = new ArrayList<String>();
        for (var col : columns) {
            if (!this.containsKey(col)) throw new IllegalStateException("col does not find in the row, col = " + col);
            if (this.get(col) == null) throw new IllegalStateException("col field is empty in the row, col = " + col);
            valueList.add(this.get(col).toString());
        }

        return String.join(delimiter, valueList);

        //  return String.join(delimiter,valueList);
    }


}
