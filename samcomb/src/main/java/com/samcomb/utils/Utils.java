package com.samcomb.utils;

import org.apache.commons.io.FileUtils;
import tech.tablesaw.api.Row;

import java.io.*;
import java.util.ArrayList;
import java.util.StringJoiner;

public class Utils {

    public static String rowToString(Row row) {
        StringJoiner sj = new StringJoiner(",");
        for (String field : row.columnNames()) {
            String data = row.getObject(field).toString();
            sj.add(data);
        }
        return sj.toString();
    }


    public static ArrayList<Integer> range(int a, int b) {
        var range = new ArrayList<Integer>();
        for (int i = a; i < b; i++) {
            range.add(i);
        }
        return range;
    }


    public static void deleteFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (file.isDirectory()) FileUtils.deleteDirectory(file);
        else file.delete();
    }

    public static boolean mkdirs(String filePath) {
        File file = new File(filePath);

        if (file.exists()) return true;

        if (file.isDirectory()) {
            return file.mkdirs();
        }
        return file.getParentFile().mkdirs();
    }

    public static File file(String path) {
        mkdirs(path);
        return new File(path);
    }


    public static void writeObjToFile(String filepath, Object obj) throws IOException {
        var out = new ObjectOutputStream(new FileOutputStream(filepath));
        out.writeObject(obj);
        out.close();
    }

    public static Object readObjectFromFile(String filepath) throws IOException, ClassNotFoundException {
        return new ObjectInputStream(new FileInputStream(filepath)).readObject();
    }

}
