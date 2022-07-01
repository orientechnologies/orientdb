package com.orientechnologies.common.thread;

import java.util.concurrent.atomic.AtomicInteger;

final class NamedThreadFactory extends BaseThreadFactory {

  private final String baseName;

  private final AtomicInteger threadId = new AtomicInteger(1);

  NamedThreadFactory(final String baseName, ThreadGroup parentThreadGroup) {
    super(parentThreadGroup);
    this.baseName = baseName;
  }

  @Override
  protected String nextThreadName() {
    return String.format("%s-%d", baseName, threadId.getAndIncrement());
  }
}
