package com.orientechnologies.agent.profiler.metrics;

/** Created by Enrico Risa on 09/07/2018. */
public interface OTimer {

  default OSnapshot getSnapshot() {
    return null;
  }

  default long getCount() {
    return 0;
  }

  default OContext time() {
    return () -> 0;
  }

  interface OContext {
    long stop();
  }
}
