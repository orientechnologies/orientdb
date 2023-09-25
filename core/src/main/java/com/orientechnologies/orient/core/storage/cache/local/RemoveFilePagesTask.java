package com.orientechnologies.orient.core.storage.cache.local;

import java.util.concurrent.Callable;

final class RemoveFilePagesTask implements Callable<Void> {
  /** */
  private final OWOWCache cache;

  private final int fileId;

  RemoveFilePagesTask(OWOWCache owowCache, final int fileId) {
    cache = owowCache;
    this.fileId = fileId;
  }

  @Override
  public Void call() {
    cache.doRemoveCachePages(fileId);
    return null;
  }
}
