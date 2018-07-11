package com.orientechnologies.common.profiler;

import com.orientechnologies.common.profiler.metrics.*;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class OrientDBProfilerStub implements OrientDBProfiler {

  @Override
  public OCounter counter(String name, String description) {
    return new OCounter() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getDescription() {
        return description;
      }
    };
  }

  @Override
  public OMeter meter(String name, String description) {
    return new OMeter() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getDescription() {
        return description;
      }
    };
  }

  @Override
  public <T> OGauge<T> gauge(String name, String description, Supplier<T> valueFunction) {
    return new OGauge<T>() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getDescription() {
        return description;
      }
    };
  }

  @Override
  public OHistogram histogram(String name, String description) {
    return new OHistogram() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getDescription() {
        return description;
      }
    };
  }

  @Override
  public OTimer timer(String name, String description) {
    return new OTimer() {
      @Override
      public OContext time() {
        return () -> 0;
      }
    };
  }

  @Override
  public Map<String, OMetric> getMetrics() {
    return Collections.emptyMap();
  }
}
