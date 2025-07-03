package com.orientechnologies.orient.core.storage;

public interface OStorageTransactionIndexKeyChanges {

  boolean isEmpty();

  Object getKey();

  Iterable<OStorageTransactionIndexChange> getOps();
}
