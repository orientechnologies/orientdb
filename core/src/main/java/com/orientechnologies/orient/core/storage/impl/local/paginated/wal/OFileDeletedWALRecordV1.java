package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public class OFileDeletedWALRecordV1 extends OFileDeletedWALRecord<OOperationUnitId> implements OperationUnitOperationId {
  public OFileDeletedWALRecordV1() {
  }

  public OFileDeletedWALRecordV1(OOperationUnitId operationUnitId, long fileId) {
    super(operationUnitId, fileId);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.FILE_DELETED_WAL_RECORD;
  }
}
