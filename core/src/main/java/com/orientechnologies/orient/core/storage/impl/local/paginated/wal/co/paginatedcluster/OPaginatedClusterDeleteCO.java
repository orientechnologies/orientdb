package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OPaginatedClusterDeleteCO extends OComponentOperationRecord {
  private String clusterName;
  private int    clusterId;

  public OPaginatedClusterDeleteCO() {
  }

  public OPaginatedClusterDeleteCO(final String clusterName, final int clusterId) {
    this.clusterName = clusterName;
    this.clusterId = clusterId;
  }

  public String getClusterName() {
    return clusterName;
  }

  public int getClusterId() {
    return clusterId;
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.addClusterInternal(clusterName, clusterId);
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.dropClusterInternal(clusterId);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(clusterId);
    OStringSerializer.INSTANCE.serializeInByteBufferObject(clusterName, buffer);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    clusterId = buffer.getInt();
    clusterName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.DELETE_CLUSTER_CO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + OStringSerializer.INSTANCE.getObjectSize(clusterName);
  }
}
