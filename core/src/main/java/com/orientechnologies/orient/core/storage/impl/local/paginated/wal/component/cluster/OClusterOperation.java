package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.OComponentOperation;

import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class OClusterOperation extends OComponentOperation {
  private int clusterId;

  OClusterOperation() {
  }

  OClusterOperation(OOperationUnitId operationUnitId, int clusterId) {
    super(operationUnitId);
    this.clusterId = clusterId;
  }

  public int getClusterId() {
    return clusterId;
  }

  @Override
  public void rollback(OAbstractPaginatedStorage storage, OAtomicOperation atomicOperation) {
    storage.rollbackClusterOperation(this, atomicOperation);
  }

  public abstract void rollbackOperation(OPaginatedCluster cluster, OAtomicOperation atomicOperation);

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);
    OIntegerSerializer.INSTANCE.serializeNative(clusterId, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);
    clusterId = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(clusterId);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    OClusterOperation that = (OClusterOperation) o;
    return clusterId == that.clusterId;
  }

  @Override
  public int hashCode() {

    return Objects.hash(super.hashCode(), clusterId);
  }
}
