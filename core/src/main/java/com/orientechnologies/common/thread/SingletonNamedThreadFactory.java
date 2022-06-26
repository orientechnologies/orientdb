package com.orientechnologies.common.thread;

final class SingletonNamedThreadFactory extends BaseThreadFactory {

  private final String name;

  SingletonNamedThreadFactory(final String name, ThreadGroup parentThreadGroup) {
    super(parentThreadGroup);
    this.name = name;
  }

  @Override
  protected String nextThreadName() {
    return name;
  }
}
