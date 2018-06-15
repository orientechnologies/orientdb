package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OPaginatedCluster;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class OCreateClusterOperation extends OClusterOperation {
  private String name;
  private long   mapFileId;

  public OCreateClusterOperation() {
  }

  public OCreateClusterOperation(final OOperationUnitId operationUnitId, final int clusterId, final String name,
      final long mapFileId) {
    super(operationUnitId, clusterId);
    this.name = name;
    this.mapFileId = mapFileId;
  }

  public String getName() {
    return name;
  }

  long getMapFileId() {
    return mapFileId;
  }

  @Override
  public void rollbackOperation(final OPaginatedCluster cluster, final OAtomicOperation atomicOperation) {
    cluster.deleteRollback();
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OStringSerializer.INSTANCE.serializeNativeObject(name, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(name);

    OLongSerializer.INSTANCE.serializeNative(mapFileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    name = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(name);

    mapFileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    OStringSerializer.INSTANCE.serializeInByteBufferObject(name, buffer);
    buffer.putLong(mapFileId);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OStringSerializer.INSTANCE.getObjectSize(name) + OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CREATE_CLUSTER_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OCreateClusterOperation that = (OCreateClusterOperation) o;
    return mapFileId == that.mapFileId && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {

    return Objects.hash(super.hashCode(), name, mapFileId);
  }
}
