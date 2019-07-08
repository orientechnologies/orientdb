package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.paginatedcluster;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OPaginatedClusterDeleteRecordCO extends OComponentOperationRecord {
  private int  clusterId;
  private long recordPosition;

  private byte[] content;
  private int    recordVersion;
  private byte   recordType;

  public OPaginatedClusterDeleteRecordCO() {
  }

  public int getClusterId() {
    return clusterId;
  }

  public long getRecordPosition() {
    return recordPosition;
  }

  public OPaginatedClusterDeleteRecordCO(final int clusterId, final long recordPosition, final byte[] content,
      final int recordVersion, final byte recordType) {
    this.clusterId = clusterId;
    this.recordPosition = recordPosition;
    this.content = content;
    this.recordVersion = recordVersion;
    this.recordType = recordType;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.deleteRecordInternal(clusterId, recordPosition);
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    storage.createRecordInternal(clusterId, content, recordVersion, recordType, recordPosition);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.putInt(clusterId);
    buffer.putLong(recordPosition);
    buffer.putInt(recordVersion);
    buffer.put(recordType);
    buffer.putInt(content.length);
    buffer.put(content);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    clusterId = buffer.getInt();
    recordPosition = buffer.getLong();
    recordVersion = buffer.getInt();
    recordType = buffer.get();

    final int contentLen = buffer.getInt();
    content = new byte[contentLen];
    buffer.get(content);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE
        + content.length;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_DELETE_RECORD_CO;
  }
}
