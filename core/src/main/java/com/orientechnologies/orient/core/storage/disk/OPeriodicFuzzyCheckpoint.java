package com.orientechnologies.orient.core.storage.disk;

import com.orientechnologies.common.log.OLogManager;

public class OPeriodicFuzzyCheckpoint implements Runnable {
  /** */
  private final OLocalPaginatedStorage storage;

  /** @param storage */
  public OPeriodicFuzzyCheckpoint(OLocalPaginatedStorage storage) {
    this.storage = storage;
  }

  @Override
  public final void run() {
    try {
      storage.makeFuzzyCheckpoint();
    } catch (final RuntimeException e) {
      OLogManager.instance().error(this, "Error during fuzzy checkpoint", e);
    }
  }
}
