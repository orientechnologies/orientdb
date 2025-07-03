package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

public interface OStorageTransactionIndexChange {

  OIdentifiable getValue();

  boolean isRemove();

  boolean isPut();
}
