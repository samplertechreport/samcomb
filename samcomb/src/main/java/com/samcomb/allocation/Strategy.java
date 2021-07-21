package com.samcomb.allocation;


import java.util.HashMap;

public class Strategy {
    public StrategyName name;
    public HashMap<String, Object> setting = new HashMap<>();
    //  double delayConstant;
    //  double lcbWeight;

    public Strategy() {
        super();
    }

    public Strategy(StrategyName name) {
        this.name = name;
        //  if (this.name == StrategyName.Lcb) setting.keySet().contains("");
        //  if (this.name == StrategyName.Eps) setting.keySet().contains("");
        //  if (this.name == StrategyName.TwoStepComb) setting.keySet().contains("");
    }

    public void addPar(String key, Object value) {
        this.setting.put(key, value);
    }

    public Object getPar(String key) {
        return this.setting.get(key);
    }

    public Object getOrElse(String key, String elseValue) {
        return this.setting.getOrDefault(key, elseValue);
    }

    public boolean containPar(String key) {
        return this.setting.containsKey(key);
    }


    public Strategy copy() {
        var strategy = new Strategy();
        strategy.name = this.name;
        strategy.setting = (HashMap<String, Object>) this.setting.clone();
        return strategy;
    }

}
