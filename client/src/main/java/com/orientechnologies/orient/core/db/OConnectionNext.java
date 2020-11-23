package com.orientechnologies.orient.core.db;

public class OConnectionNext {
  private volatile int next = 0;
  private final int limit;

  public OConnectionNext(int limit) {
    this.limit = limit;
  }

  public synchronized int next() {
    if (next < limit) {
      next++;
    } else {
      next = 0;
    }
    return next;
  }
}
