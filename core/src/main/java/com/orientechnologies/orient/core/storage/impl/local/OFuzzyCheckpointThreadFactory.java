package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.util.OUncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;

final class OFuzzyCheckpointThreadFactory implements ThreadFactory {
  @Override
  public Thread newThread(final Runnable r) {
    final Thread thread = new Thread(OAbstractPaginatedStorage.storageThreadGroup, r);
    thread.setDaemon(true);
    thread.setUncaughtExceptionHandler(new OUncaughtExceptionHandler());
    return thread;
  }
}
