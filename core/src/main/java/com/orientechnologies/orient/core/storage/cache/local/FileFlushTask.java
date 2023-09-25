package com.orientechnologies.orient.core.storage.cache.local;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

final class FileFlushTask implements Callable<Void> {
  /** */
  private final OWOWCache cache;

  private final Set<Integer> fileIdSet;

  FileFlushTask(OWOWCache owowCache, final Collection<Integer> fileIds) {
    cache = owowCache;
    this.fileIdSet = new HashSet<>(fileIds);
  }

  @Override
  public Void call() throws Exception {
    return cache.executeFileFlush(this.fileIdSet);
  }
}
