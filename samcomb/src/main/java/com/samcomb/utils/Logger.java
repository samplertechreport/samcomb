package com.samcomb.utils;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class Logger {
    public static final org.slf4j.Logger logger = LoggerFactory.getLogger("global");

    public static org.slf4j.Logger getGlobalLogger() {
        return logger;
    }


    private static boolean enabled = true;
    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    @Deprecated
    public static void log(Object... objs) throws IOException {
        String delimiter = ",";
        var objStrs = Arrays.stream(objs).map(x -> x.toString()).collect(Collectors.toList());
        String info = String.join(delimiter, objStrs);

        if (enabled) {

            var callerStack = Thread.currentThread().getStackTrace()[2];
            var callerInfo = String.format("%s:%d,%s.%s",
                    callerStack.getFileName(),
                    callerStack.getLineNumber(),
                    callerStack.getClassName(),
                    callerStack.getMethodName()
            );
            var callTime = format.format(new Date());
            var line = String.format("[%s][%s] %s", callTime, callerInfo, info);
            System.out.println(line);

            //FileWriter fw = new FileWriter("log.txt", true);
            //fw.write(line);
            //fw.close();

        }
    }
}
