package com.samcomb.dataframe;

import java.util.Arrays;
import java.util.HashSet;

public enum DataType {
    t_bool,
    t_numeric,
    t_char,
    t_bpchar,
    t_int2,
    t_int4,
    t_int8,
    t_float4,
    t_float8,
    t_varchar,
    t_text,
    t_name,
    t_bytea,
    t_date,
    t_time,
    t_timetz,
    t_timestamp,
    t_timestamptz,
    t_INTEGER,
    t_REAL,
    t_TEXT,
    t_serial,
    t_object;

    public static DataType getDataType(String typename) {
        return DataType.valueOf("t_" + typename);
    }

    public static boolean isNumberic(DataType dataType) {
        DataType arr[] = {t_numeric, t_int2, t_int4, t_int8, t_float4, t_float8, t_INTEGER, t_REAL, t_serial};
        var numeric = new HashSet<DataType>(Arrays.asList(arr));
        return numeric.contains(dataType);
    }

    public static boolean isCategorical(DataType dataType) {
        return (!isNumberic(dataType));
    }

}
