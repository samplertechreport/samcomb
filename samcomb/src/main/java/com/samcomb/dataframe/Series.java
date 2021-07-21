package com.samcomb.dataframe;

import java.util.ArrayList;
import java.util.HashSet;

public class Series<E> extends ArrayList<E> {
    String name;
    DataType dataType;

    public Series(String name, DataType dataType) {
        super();
        this.name = name;
        this.dataType = dataType;
    }

    public <T> Series<T> cast() {
        return (Series<T>) (this);
    }


    public Series<Number> toNumberSeries() {
        return (Series<Number>) (this);
    }

    public Series<Double> toDoubleSeries() {
        return (Series<Double>) (this);
    }


    public Series<Double> parseToDoubleSeries() {
        var series = new Series<Double>(this.name, this.dataType);
        for (var r : this) {
            var d = Double.valueOf(r.toString());
            series.add(d);
        }
        return series;
    }


    public Series<Integer> parseToIntegerSeries() {
        var series = new Series<Integer>(this.name, this.dataType);
        for (var r : this) {
            var d = Integer.valueOf(r.toString());
            series.add(d);
        }
        return series;
    }


    public HashSet<E> distinct() {
        var set = new HashSet<E>();
        set.addAll(this);
        return set;
    }

    public boolean isNumeric() {
        return DataType.isNumberic(this.dataType);
    }

    public boolean isCategorical() {
        return DataType.isCategorical(this.dataType);
    }


    public Series<String> toStringSeries() {
        return (Series<String>) (this);
    }

    public Series<Integer> toIntSeries() {
        return (Series<Integer>) (this);
    }

    public Series<Long> toLongSeries() {
        return (Series<Long>) (this);
    }


    /*
    public Series<?> toDoubleSeries()
    {
        if (DataType.isNumberic(this.dataType))
        {
            var series = new Series<>();

        }
    }
    */

}
