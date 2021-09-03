package com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations;

public enum AtomicOperationStatus {
  NOT_STARTED,
  IN_PROGRESS,
  COMMITTED,
  ROLLED_BACK,
  PERSISTED
}
