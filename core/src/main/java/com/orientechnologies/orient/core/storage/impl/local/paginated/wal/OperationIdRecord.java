package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public interface OperationIdRecord<T> {
  T getOperationUnitId();
}
