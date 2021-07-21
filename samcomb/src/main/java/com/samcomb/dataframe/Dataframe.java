package com.samcomb.dataframe;

import com.samcomb.utils.Logger;
import com.samcomb.utils.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class Dataframe extends LinkedHashMap<String, Series<Object>> {

    public static HashMap<String, Object> sqlCache = new HashMap<String, Object>();

    public Dataframe() {
        super();
    }


    public Dataframe(List<String> colNames, List<DataType> dataType) {
        super();
        assert colNames.size() == dataType.size();
        int numCols = colNames.size();
        for (int i = 0; i < numCols; i++) {
            var col = colNames.get(i);
            var series = new Series(col, dataType.get(i));

            // var series = new Series<>(colNames.get(i), dataType.get(i));
            this.put(col, series);
        }
    }

    public void insert(Row row) {
        if (this.isEmpty()) {
            for (var col : row.keySet()) {
                var series = new Series(col, DataType.t_object);
                this.put(col, series);
            }
        }

        assert row.keySet().equals(this.keySet()) : "inserted row should have the same schema with existing dataframe";
        for (var col : row.keySet()) {
            var data = row.get(col);
            var series = this.get(col);
            series.add(data);
        }
    }


    public void insert(Dataframe df) {
        int n = df.getRowNum();
        if (n == 0) Logger.logger.warn("the inserted rows is 0");
        for (var i = 0; i < n; i++) {
            var row = df.getRow(i);
            this.insert(row);
        }
    }


    public int getColumnCount() {
        return this.keySet().size();
    }


    public Set<String> getColumnNames() {
        return this.keySet();
    }


    @Override
    public Series<Object> get(Object key) {
        if (!super.containsKey(key)) throw new IllegalArgumentException(String.format("column %s does not exist", key));
        return super.get(key);
    }

    public Row getRow(int index) {
        var colNames = this.keySet();
        var row = new Row();
        for (var col : colNames) {
            var series = this.get(col);
            var data = series.get(index);
            row.put(col, data);
        }
        return row;
    }

    public int getRowNum() {
        if (this.values().isEmpty() || (!this.values().iterator().hasNext())) return 0;
        int n = this.values().iterator().next().size();
        for (var series : this.values()) {
            assert series.size() == n;
        }

        return n;
    }


    public static Dataframe readCsv(String path) throws IOException {

        var csvFile = new FileInputStream(path);
        InputStreamReader input = new InputStreamReader(csvFile);
        CSVParser csvParser = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(input);
        var columns = csvParser.getHeaderNames();
        var dataTypes = new ArrayList<DataType>();
        for (var col : columns) {
            dataTypes.add(DataType.t_text);
        }
        var dataframe = new Dataframe(columns, dataTypes);
        for (var record : csvParser) {
            var row = new Row();
            for (var col : columns) {
                var data = record.get(col);
                row.put(col, data);
            }
            dataframe.insert(row);
        }

        return dataframe;
    }

    public static Object getSingleValueFromSql(String sql, Connection conn,
                                               Boolean useCache)
            throws SQLException, IOException {

        if (useCache && sqlCache.containsKey(sql)) {
            Logger.logger.debug("use cached sql result:" + sql);
            return sqlCache.get(sql);
        }

        Logger.logger.debug("issue sql:" + sql);
        var stat = conn.createStatement();
        var resultSet = stat.executeQuery(sql);
        resultSet.next();
        if (resultSet.getMetaData().getColumnCount() != 1) throw new IllegalStateException("column number should be 1");
        Object value = resultSet.getObject(1);
        if (resultSet.next()) throw new IllegalStateException("the result is supposed to be a single value");
        stat.close();
        if (useCache) {
            sqlCache.put(sql, value);
        }
        return value;
    }


    public static Dataframe fromSql(Connection conn, String sql) throws SQLException, IOException {

        // Logger.logger.info("issue sql:" + sql);
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql);

        var meta = rs.getMetaData();
        var numCols = meta.getColumnCount();
        var colNames = new ArrayList<String>();
        var dataTypes = new ArrayList<DataType>();

        //note that sql starts from 1
        for (int i = 1; i <= numCols; i++) {
            var name = meta.getColumnName(i);
            var typename = meta.getColumnTypeName(i);
            colNames.add(name);
            dataTypes.add(DataType.getDataType(typename));
        }

        var dataframe = new Dataframe(colNames, dataTypes);

        while (rs.next()) {
            var row = new Row();
            for (var col : colNames) {
                var data = rs.getObject(col);
                row.put(col, data);
            }

            dataframe.insert(row);
        }

        if (stmt != null) {
            stmt.close();
        }
        if (rs != null) {
            rs.close();
        }
        return dataframe;
    }


    public void show() throws IOException {
        show(20);
    }

    public void show(int n) throws IOException {
        var s = toString(n);
        Logger.logger.info(s);
    }


    public String toString() {
        //default show 20 rows.
        return toString(20);
    }

    public String toString(int n) {
        String s = "";
        s += "\n-------------------------show dataframe----------------\n";
        s += "row number = " + this.getRowNum() + "\n";
        var columns = new ArrayList<>(this.keySet());
        String header = String.join(", ", columns);
        s += header + "\n";

        //  s+= this.keySet().toString() + "\n";
        var showNum = Math.min(n, this.getRowNum());
        for (int i = 0; i < showNum; i++) {
            var row = this.getRow(i);
            s += row.toString(columns, ", ") + "\n";
        }

        s += "------------------------show end--------------------\n";
        return s;
    }


    public void save(String path) throws IOException {

        FileWriter fw = new FileWriter(Utils.file(path));
        var columns = new ArrayList<>(this.keySet());
        String header = String.join(",", columns);
        fw.write(header + "\n");
        int n = this.getRowNum();
        if (n == 0) Logger.logger.warn("empty dataframe, write nothing to file {}", path);
        for (int i = 0; i < n; i++) {
            var row = this.getRow(i);
            var s = row.toString(columns, ",");
            fw.write(s + "\n");
        }
        fw.close();
    }

}
