package com.orientechnologies.orient.core.storage.cache.local;

import java.util.concurrent.CountDownLatch;

final class ExclusiveFlushTask implements Runnable {
  /** */
  private final OWOWCache cache;

  private final CountDownLatch cacheBoundaryLatch;
  private final CountDownLatch completionLatch;

  ExclusiveFlushTask(
      OWOWCache owowCache,
      final CountDownLatch cacheBoundaryLatch,
      final CountDownLatch completionLatch) {
    cache = owowCache;
    this.cacheBoundaryLatch = cacheBoundaryLatch;
    this.completionLatch = completionLatch;
  }

  @Override
  public void run() {
    cache.executeFlush(cacheBoundaryLatch, completionLatch);
  }
}
