package com.orientechnologies.orient.core.storage.cache.local;

import java.util.concurrent.Callable;

final class FindMinDirtySegment implements Callable<Long> {
  /** */
  private final OWOWCache cache;

  /**
   * @param owowCache
   */
  FindMinDirtySegment(OWOWCache owowCache) {
    cache = owowCache;
  }

  @Override
  public Long call() {
    return cache.executeFindDirtySegment();
  }
}
