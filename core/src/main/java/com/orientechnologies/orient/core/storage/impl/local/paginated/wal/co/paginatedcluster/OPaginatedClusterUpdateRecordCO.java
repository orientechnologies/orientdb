package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class OPaginatedClusterUpdateRecordCO extends OComponentOperationRecord {
  private int  clusterId;
  private long clusterPosition;

  private byte[] recordContent;
  private int    recordVersion;
  private byte   recordType;

  private byte[] oldRecordContent;
  private int    oldRecordVersion;
  private byte   oldRecordType;

  public OPaginatedClusterUpdateRecordCO() {
  }

  public OPaginatedClusterUpdateRecordCO(final int clusterId, final long clusterPosition, final byte[] recordContent,
      final int recordVersion, final byte recordType, final byte[] oldRecordContent, final int oldRecordVersion,
      final byte oldRecordType) {
    this.clusterId = clusterId;
    this.clusterPosition = clusterPosition;
    this.recordContent = recordContent;
    this.recordVersion = recordVersion;
    this.recordType = recordType;
    this.oldRecordContent = oldRecordContent;
    this.oldRecordVersion = oldRecordVersion;
    this.oldRecordType = oldRecordType;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.updateRecordInternal(clusterId, clusterPosition, recordContent, recordVersion, recordType);
  }

  public int getClusterId() {
    return clusterId;
  }

  public long getClusterPosition() {
    return clusterPosition;
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

  byte[] getOldRecordContent() {
    return oldRecordContent;
  }

  int getOldRecordVersion() {
    return oldRecordVersion;
  }

  byte getOldRecordType() {
    return oldRecordType;
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.updateRecordInternal(clusterId, clusterPosition, oldRecordContent, oldRecordVersion, oldRecordType);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(clusterId);
    buffer.putLong(clusterPosition);

    buffer.putInt(recordContent.length);
    buffer.put(recordContent);

    buffer.putInt(recordVersion);
    buffer.put(recordType);

    buffer.putInt(oldRecordContent.length);
    buffer.put(oldRecordContent);

    buffer.putInt(oldRecordVersion);
    buffer.put(oldRecordType);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    clusterId = buffer.getInt();
    clusterPosition = buffer.getLong();

    final int contentLen = buffer.getInt();
    recordContent = new byte[contentLen];
    buffer.get(recordContent);

    recordVersion = buffer.getInt();
    recordType = buffer.get();

    final int oldRecordContentLen = buffer.getInt();
    oldRecordContent = new byte[oldRecordContentLen];
    buffer.get(oldRecordContent);

    oldRecordVersion = buffer.getInt();
    oldRecordType = buffer.get();
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE + 2 * OByteSerializer.BYTE_SIZE
        + recordContent.length + oldRecordContent.length;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_UPDATE_RECORD_CO;
  }
}
