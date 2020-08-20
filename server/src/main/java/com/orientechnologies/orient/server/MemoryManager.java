package com.orientechnologies.orient.server;

public interface MemoryManager {
  void start();

  void shutdown();

  void checkAndWaitMemoryThreshold();
}
