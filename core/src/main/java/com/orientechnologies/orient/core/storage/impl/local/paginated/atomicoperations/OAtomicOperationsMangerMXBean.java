package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

/**
 * Created by lomak_000 on 7/17/2015.
 */
public interface OAtomicOperationsMangerMXBean {
  void trackAtomicOperations();
  void doNotTrackAtomicOperations();

  String dumpActiveAtomicOperations();
}
