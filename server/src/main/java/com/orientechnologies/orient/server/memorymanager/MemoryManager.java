package com.orientechnologies.orient.server.memorymanager;

public interface MemoryManager {
  void start();

  void shutdown();

  void checkAndWaitMemoryThreshold();
}
