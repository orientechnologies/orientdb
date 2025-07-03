package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.id.ORID;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;

public interface OStorageTransaction {

  int getId();

  Optional<byte[]> getMetadata();

  void updateCache(boolean keepInCache);

  Iterator<byte[]> getSerializedOperations();

  void storageTransaction();

  Collection<OStorageRecordOperation> getRecordChanges();

  Set<ORID> getLockedRecords();

  void updateIdentityAfterCommit(ORID oldRID, ORID rid);

  SortedMap<String, OStorageTransactionIndexChanges> getChangesForIndex();
}
