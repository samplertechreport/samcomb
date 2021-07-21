package com.samcomb.experiment.exp;

import java.text.SimpleDateFormat;
import java.util.Date;

public class ExpTool {

    public static String getDatePrefix() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss");
        String datePrefix = format.format(new Date());
        return datePrefix;
    }

}
