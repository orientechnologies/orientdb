package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OPaginatedClusterAllocatePositionCO extends OComponentOperationRecord {
  private int  clusterId;
  private byte recordType;

  public OPaginatedClusterAllocatePositionCO() {
  }

  public OPaginatedClusterAllocatePositionCO(final int clusterId, final byte recordType) {
    this.clusterId = clusterId;
    this.recordType = recordType;
  }

  public int getClusterId() {
    return clusterId;
  }

  public byte getRecordType() {
    return recordType;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putShort((short) clusterId);
    buffer.put(recordType);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    this.clusterId = buffer.getShort();
    this.recordType = buffer.get();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OShortSerializer.SHORT_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.allocatePositionInternal(clusterId, recordType);
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) {
    //do nothing
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_ALLOCATE_RECORD_POSITION_CO;
  }
}
