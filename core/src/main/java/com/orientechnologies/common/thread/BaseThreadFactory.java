package com.orientechnologies.common.thread;

import java.util.concurrent.ThreadFactory;

abstract class BaseThreadFactory implements ThreadFactory {

  private final ThreadGroup parentThreadGroup;

  protected BaseThreadFactory(ThreadGroup parentThreadGroup) {
    this.parentThreadGroup = parentThreadGroup;
  }

  @Override
  public final Thread newThread(final Runnable r) {
    final Thread thread = new Thread(parentThreadGroup, r);
    thread.setDaemon(true);
    thread.setName(nextThreadName());
    return thread;
  }

  protected abstract String nextThreadName();
}
