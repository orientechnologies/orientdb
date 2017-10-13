package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

public interface OAtomicOperationsMangerMXBean {
  void trackAtomicOperations();
  void doNotTrackAtomicOperations();

  String dumpActiveAtomicOperations();
}
