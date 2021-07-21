package com.samcomb.sampler;

import java.util.List;
import java.util.Map;

public interface DataFetcher {

    void reset();

    Map<String, List<Double>> getNext(int size);
}
