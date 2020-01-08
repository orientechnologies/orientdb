package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

public class OFileTruncatedWALRecordV2 extends OFileTruncatedWALRecord<Long> implements LongOperationId {
  public OFileTruncatedWALRecordV2() {
  }

  public OFileTruncatedWALRecordV2(Long operationUnitId, long fileId) {
    super(operationUnitId, fileId);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.FILE_TRUNCATED_WAL_RECORD_V2;
  }
}
