package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;

import java.nio.ByteBuffer;

public class OUpdateRecordOperation extends OClusterOperation {
  private long clusterPosition;

  private byte[] record;
  private int    recordVersion;
  private byte   recordType;

  private byte[] prevRecord;
  private int    prevRecordVersion;
  private byte   prevRecordType;

  public OUpdateRecordOperation() {
  }

  public OUpdateRecordOperation(OOperationUnitId operationUnitId, int clusterId, long clusterPosition, byte[] record,
      int recordVersion, byte recordType, byte[] prevRecord, int prevRecordVersion, byte prevRecordType) {
    super(operationUnitId, clusterId);

    this.clusterPosition = clusterPosition;

    this.record = record;
    this.recordVersion = recordVersion;
    this.recordType = recordType;

    this.prevRecord = prevRecord;
    this.prevRecordVersion = prevRecordVersion;
    this.prevRecordType = prevRecordType;
  }

  public long getClusterPosition() {
    return clusterPosition;
  }

  public byte[] getRecord() {
    return record;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public byte getRecordType() {
    return recordType;
  }

  public byte[] getPrevRecord() {
    return prevRecord;
  }

  public int getPrevRecordVersion() {
    return prevRecordVersion;
  }

  public byte getPrevRecordType() {
    return prevRecordType;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(clusterPosition, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(record.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(record, 0, content, offset, record.length);
    offset += record.length;

    OIntegerSerializer.INSTANCE.serializeNative(recordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = recordType;
    offset++;

    OIntegerSerializer.INSTANCE.serializeNative(prevRecord.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    System.arraycopy(prevRecord, 0, content, offset, prevRecord.length);
    offset += prevRecord.length;

    OIntegerSerializer.INSTANCE.serializeNative(prevRecordVersion, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    content[offset] = prevRecordType;
    offset++;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    clusterPosition = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    int recordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordLen];
    System.arraycopy(content, offset, record, 0, recordLen);
    offset += recordLen;

    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordType = content[offset];
    offset++;

    int prevRecordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevRecord = new byte[prevRecordLen];
    System.arraycopy(content, offset, prevRecord, 0, prevRecordLen);
    offset += prevRecordLen;

    prevRecordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    prevRecordType = content[offset];
    offset++;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(clusterPosition);

    buffer.putInt(record.length);
    buffer.put(record);
    buffer.putInt(recordVersion);
    buffer.put(recordType);

    buffer.putInt(prevRecord.length);
    buffer.put(prevRecord);
    buffer.putInt(prevRecordVersion);
    buffer.put(prevRecordType);
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();
    size += OLongSerializer.LONG_SIZE;

    size += OIntegerSerializer.INT_SIZE;
    size += record.length;
    size += OIntegerSerializer.INT_SIZE;
    size += OByteSerializer.BYTE_SIZE;

    size += OIntegerSerializer.INT_SIZE;
    size += prevRecord.length;
    size += OIntegerSerializer.INT_SIZE;
    size += OByteSerializer.BYTE_SIZE;

    return size;
  }
}
