package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPositionMapBucketTruncateLastEntryPO extends PageOperationRecord {
  private byte recordStatus;
  private int  recordPageIndex;
  private int  recordPosition;

  public ClusterPositionMapBucketTruncateLastEntryPO() {
  }

  public ClusterPositionMapBucketTruncateLastEntryPO(byte recordStatus, int recordPageIndex, int recordPosition) {
    this.recordStatus = recordStatus;
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
  }

  public byte getRecordStatus() {
    return recordStatus;
  }

  public int getRecordPageIndex() {
    return recordPageIndex;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    bucket.truncateLastEntry();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    bucket.add(recordPageIndex, recordPosition, recordStatus);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_TRUNCATE_LAST_ENTRY_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(recordStatus);
    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    recordStatus = buffer.get();
    recordPageIndex = buffer.getInt();
    recordPosition = buffer.getInt();
  }
}
