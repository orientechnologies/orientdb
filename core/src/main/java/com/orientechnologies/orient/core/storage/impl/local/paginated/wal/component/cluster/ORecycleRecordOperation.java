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

public final class ORecycleRecordOperation extends OClusterOperation {
  private long   clusterPosition;
  private byte[] record;
  private int    recordVersion;
  private byte   recordType;

  public ORecycleRecordOperation() {
  }

  public ORecycleRecordOperation(final OOperationUnitId operationUnitId, final int clusterId, final long clusterPosition,
      final byte[] record, final int recordVersion, final byte recordType) {
    super(operationUnitId, clusterId);
    this.clusterPosition = clusterPosition;
    this.record = record;
    this.recordVersion = recordVersion;
    this.recordType = recordType;
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

  @Override
  public void rollbackOperation(final OPaginatedCluster cluster, final OAtomicOperation atomicOperation) {
    throw new UnsupportedOperationException("Recycle operations are not inteded to be rolled back");
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
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();

    size += OLongSerializer.LONG_SIZE;
    size += OIntegerSerializer.INT_SIZE;
    size += record.length;
    size += OIntegerSerializer.INT_SIZE;
    size += OByteSerializer.BYTE_SIZE;

    return size;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.RECYCLE_RECORD_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final ORecycleRecordOperation that = (ORecycleRecordOperation) o;
    return clusterPosition == that.clusterPosition && recordVersion == that.recordVersion && recordType == that.recordType && Arrays
        .equals(record, that.record);
  }

  @Override
  public int hashCode() {

    int result = Objects.hash(super.hashCode(), clusterPosition, recordVersion, recordType);
    result = 31 * result + Arrays.hashCode(record);
    return result;
  }
}
