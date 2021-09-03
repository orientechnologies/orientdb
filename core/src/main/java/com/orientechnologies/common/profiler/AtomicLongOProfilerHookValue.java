package com.orientechnologies.common.profiler;

import java.util.concurrent.atomic.AtomicLong;

/** Created by tglman on 03/05/17. */
public class AtomicLongOProfilerHookValue implements OAbstractProfiler.OProfilerHookValue {
  private final AtomicLong value;

  public AtomicLongOProfilerHookValue(AtomicLong value) {
    this.value = value;
  }

  @Override
  public Object getValue() {
    return value.get();
  }
}
