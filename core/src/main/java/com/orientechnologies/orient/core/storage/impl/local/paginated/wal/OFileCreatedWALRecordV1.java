package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public class OFileCreatedWALRecordV1 extends OFileCreatedWALRecord<OOperationUnitId> implements OperationUnitOperationId {
  public OFileCreatedWALRecordV1() {
  }

  public OFileCreatedWALRecordV1(OOperationUnitId operationUnitId, String fileName, long fileId) {
    super(operationUnitId, fileName, fileId);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.FILE_CREATED_WAL_RECORD;
  }
}
