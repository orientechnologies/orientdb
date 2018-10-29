package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class OUpdateRecordOperation extends OClusterOperation {
  private long clusterPosition;

  private byte[] record;
  private int    recordVersion;
  private byte   recordType;

  private byte[] prevRecord;
  private int    prevRecordVersion;
  private byte   prevRecordType;

  public OUpdateRecordOperation() {
  }

  public OUpdateRecordOperation(final OOperationUnitId operationUnitId, final int clusterId, final long clusterPosition,
      final byte[] record, final int recordVersion, final byte recordType, final byte[] prevRecord, final int prevRecordVersion,
      final byte prevRecordType) {
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

  byte[] getPrevRecord() {
    return prevRecord;
  }

  int getPrevRecordVersion() {
    return prevRecordVersion;
  }

  byte getPrevRecordType() {
    return prevRecordType;
  }

  @Override
  public void rollbackOperation(final OPaginatedCluster cluster, final OAtomicOperation atomicOperation) {
    cluster.updateRecordRollback(clusterPosition, prevRecord, prevRecordVersion, prevRecordType, atomicOperation);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
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
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    clusterPosition = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int recordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    record = new byte[recordLen];
    System.arraycopy(content, offset, record, 0, recordLen);
    offset += recordLen;

    recordVersion = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    recordType = content[offset];
    offset++;

    final int prevRecordLen = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
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
  public void toStream(final ByteBuffer buffer) {
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

  @Override
  public byte getId() {
    return WALRecordTypes.UPDATE_RECORD_OPERATION;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    OUpdateRecordOperation that = (OUpdateRecordOperation) o;
    return clusterPosition == that.clusterPosition && recordVersion == that.recordVersion && recordType == that.recordType
        && prevRecordVersion == that.prevRecordVersion && prevRecordType == that.prevRecordType && Arrays
        .equals(record, that.record) && Arrays.equals(prevRecord, that.prevRecord);
  }

  @Override
  public int hashCode() {

    int result = Objects.hash(super.hashCode(), clusterPosition, recordVersion, recordType, prevRecordVersion, prevRecordType);
    result = 31 * result + Arrays.hashCode(record);
    result = 31 * result + Arrays.hashCode(prevRecord);
    return result;
  }
}
