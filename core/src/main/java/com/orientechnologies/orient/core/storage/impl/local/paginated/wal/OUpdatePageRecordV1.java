package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public final class OUpdatePageRecordV1 extends OUpdatePageRecord<OOperationUnitId> implements OperationUnitOperationId {
  public OUpdatePageRecordV1() {
  }

  public OUpdatePageRecordV1(long pageIndex, long fileId, OOperationUnitId operationUnitId, OWALChanges changes) {
    super(pageIndex, fileId, operationUnitId, changes);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.UPDATE_PAGE_RECORD;
  }
}
