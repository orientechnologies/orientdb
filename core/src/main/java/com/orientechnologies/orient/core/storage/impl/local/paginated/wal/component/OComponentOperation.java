package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitBodyRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

public abstract class OComponentOperation extends OOperationUnitBodyRecord {
  protected OComponentOperation() {
  }

  public OComponentOperation(OOperationUnitId operationUnitId) {
    super(operationUnitId);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  public abstract void rollback(OAbstractPaginatedStorage storage, OAtomicOperation atomicOperation);

}
