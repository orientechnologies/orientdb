package com.orientechnologies.orient.core.storage;

import com.orientechnologies.orient.core.id.ORID;
import java.util.Collection;

public interface OAllocationTransaction {

  Collection<OStorageRecordOperation> getRecordChanges();

  void updateIdentityAfterCommit(ORID oldRID, ORID rid);
}
