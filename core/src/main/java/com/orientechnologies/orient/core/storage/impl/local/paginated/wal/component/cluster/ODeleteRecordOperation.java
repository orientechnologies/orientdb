package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class ODeleteRecordOperation extends OClusterOperation {
  private long clusterPosition;

  private byte[] record;
  private int    recordVersion;
  private byte   recordType;

  public ODeleteRecordOperation() {
  }

  public ODeleteRecordOperation(final OOperationUnitId operationUnitId, final int clusterId, final long clusterPosition,
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
    cluster.recycleRecordRollback(clusterPosition, record, recordVersion, recordType, atomicOperation);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(clusterPosition, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    clusterPosition = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(clusterPosition);
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();

    size += OLongSerializer.LONG_SIZE;

    return size;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.DELETE_RECORD_OPERATION;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    ODeleteRecordOperation that = (ODeleteRecordOperation) o;
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

