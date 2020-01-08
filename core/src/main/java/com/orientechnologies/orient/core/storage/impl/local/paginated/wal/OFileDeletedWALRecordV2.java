package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public class OFileDeletedWALRecordV2 extends OFileDeletedWALRecord<Long> implements LongOperationId {
  public OFileDeletedWALRecordV2() {
  }

  public OFileDeletedWALRecordV2(Long operationUnitId, long fileId) {
    super(operationUnitId, fileId);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.FILE_DELETED_WAL_RECORD_V2;
  }
}
