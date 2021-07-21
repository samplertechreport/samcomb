package com.samcomb.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Config {
    public HashMap<String, Object> data;

    public Config() {
        super();
        data = new HashMap<>();
    }

    public void add(String key, Object value) {
        this.data.put(key, value);
    }

    public Config copy() {
        var config = new Config();
        config.data = (HashMap<String, Object>) this.data.clone();
        return config;
    }

    public Object getObject(String key) {
        return this.data.get(key);
    }

    public String getString(String key) {
        return (String) this.getObject(key);
    }

    public double getDouble(String key) {
        return (double) this.getObject(key);
    }

    public List<String> getStringList(String key) {
        return (List<String>) this.getObject(key);
    }

    public List<Double> getDoubleList(String key) {
        return (List<Double>) this.getObject(key);
    }

    public List<Integer> getIntegerList(String key) {
        return (List<Integer>) this.getObject(key);
    }

    public int getInt(String key) {
        return (int) this.getObject(key);
    }


    public boolean contains(String key) {
        return this.data.containsKey(key);
    }

    public Config getConfig(String key) {
        return (Config) this.getObject(key);
    }

    public boolean getBoolean(String key) {
        return (boolean) this.getObject(key);
    }


    public void validateNotNull() {
        var nullParams = new ArrayList<String>();
        for (var s : data.keySet()) {
            if (data.get(s) == null) nullParams.add(s);
        }
        if (nullParams.size() > 0) {
            throw new IllegalArgumentException("The following paramaters can not be null:" + String.join(",", nullParams));
        }
    }
}
