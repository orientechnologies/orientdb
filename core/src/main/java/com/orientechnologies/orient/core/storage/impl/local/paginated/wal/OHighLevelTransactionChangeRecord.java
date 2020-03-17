package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.nio.ByteBuffer;

import static com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes.HIGH_LEVEL_TRANSACTION_CHANGE_RECORD;

public class OHighLevelTransactionChangeRecord extends OOperationUnitRecord {
  private byte[] data;

  public OHighLevelTransactionChangeRecord() {

  }

  public OHighLevelTransactionChangeRecord(OOperationUnitId operationUnitId, byte[] data) {
    super(operationUnitId);
    this.data = data;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    buffer.putInt(data.length);
    buffer.put(data, 0, data.length);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    int size = buffer.getInt();
    data = new byte[size];
    buffer.get(data, 0, size);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }

  @Override
  public int getId() {
    return HIGH_LEVEL_TRANSACTION_CHANGE_RECORD;
  }

  public byte[] getData() {
    return data;
  }
}
