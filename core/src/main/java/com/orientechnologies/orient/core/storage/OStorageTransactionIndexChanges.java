package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.index.OIndexInternal;
import java.util.SortedMap;

public interface OStorageTransactionIndexChanges {

  OIndexInternal getAssociatedIndex();

  boolean isClearIndex();

  OStorageTransactionIndexKeyChanges getNullChanges();

  SortedMap<Object, OStorageTransactionIndexKeyChanges> getChanges();
}
