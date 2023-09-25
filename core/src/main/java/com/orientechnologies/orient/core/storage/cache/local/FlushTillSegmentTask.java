package com.orientechnologies.orient.core.storage.cache.local;

import java.util.concurrent.Callable;

final class FlushTillSegmentTask implements Callable<Void> {
  /** */
  private final OWOWCache cache;

  private final long segmentId;

  FlushTillSegmentTask(OWOWCache owowCache, final long segmentId) {
    cache = owowCache;
    this.segmentId = segmentId;
  }

  @Override
  public Void call() throws Exception {
    return cache.executeFlushTillSegment(this.segmentId);
  }
}
