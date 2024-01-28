package com.orientechnologies.orient.core.storage.cache.local;

final class PeriodicFlushTask implements Runnable {

  /** */
  private final OWOWCache owowCache;

  /**
   * @param owowCache
   */
  public PeriodicFlushTask(OWOWCache owowCache) {
    this.owowCache = owowCache;
  }

  @Override
  public void run() {
    this.owowCache.executePeriodicFlush(this);
  }
}
