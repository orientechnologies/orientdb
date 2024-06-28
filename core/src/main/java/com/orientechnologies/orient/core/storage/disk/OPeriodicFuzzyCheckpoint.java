package com.orientechnologies.orient.core.storage.disk;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;

public class OPeriodicFuzzyCheckpoint implements Runnable {
  private static final OLogger logger =
      OLogManager.instance().logger(OPeriodicFuzzyCheckpoint.class);

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
      logger.error("Error during fuzzy checkpoint", e);
    }
  }
}
