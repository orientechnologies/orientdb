package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OPaginatedClusterCreateRecordCO extends OComponentOperationRecord {
  private int    clusterId;
  private byte[] recordContent;
  private int    recordVersion;
  private byte   recordType;
  private long   allocatedPosition;
  private long   recordPosition;

  public OPaginatedClusterCreateRecordCO() {
  }

  public OPaginatedClusterCreateRecordCO(final int clusterId, final byte[] recordContent, final int recordVersion,
      final byte recordType, final long allocatedPosition, final long recordPosition) {
    this.clusterId = clusterId;
    this.recordContent = recordContent;
    this.recordVersion = recordVersion;
    this.recordType = recordType;
    this.allocatedPosition = allocatedPosition;
    this.recordPosition = recordPosition;
  }

  public int getClusterId() {
    return clusterId;
  }

  public byte[] getRecordContent() {
    return recordContent;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public byte getRecordType() {
    return recordType;
  }

  long getAllocatedPosition() {
    return allocatedPosition;
  }

  public long getRecordPosition() {
    return recordPosition;
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.deleteRecordInternal(clusterId, recordPosition);
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.createRecordInternal(clusterId, recordContent, recordVersion, recordType, allocatedPosition);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(clusterId);
    buffer.putInt(recordContent.length);
    buffer.put(recordContent);
    buffer.put(recordType);
    buffer.putInt(recordVersion);
    buffer.putLong(allocatedPosition);
    buffer.putLong(recordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    this.clusterId = buffer.getInt();

    final int contentSize = buffer.getInt();
    recordContent = new byte[contentSize];

    buffer.get(recordContent);
    recordType = buffer.get();
    recordVersion = buffer.getInt();
    allocatedPosition = buffer.getLong();
    recordPosition = buffer.getLong();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + recordContent.length + OByteSerializer.BYTE_SIZE
        + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_CREATE_RECORD_CO;
  }
}
