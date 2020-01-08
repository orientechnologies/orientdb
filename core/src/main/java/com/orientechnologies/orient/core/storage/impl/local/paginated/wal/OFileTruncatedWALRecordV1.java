package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public class OFileTruncatedWALRecordV1 extends OFileTruncatedWALRecord<OOperationUnitId> implements OperationUnitOperationId {
  public OFileTruncatedWALRecordV1() {
  }

  public OFileTruncatedWALRecordV1(OOperationUnitId operationUnitId, long fileId) {
    super(operationUnitId, fileId);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.FILE_TRUNCATED_WAL_RECORD;
  }
}
