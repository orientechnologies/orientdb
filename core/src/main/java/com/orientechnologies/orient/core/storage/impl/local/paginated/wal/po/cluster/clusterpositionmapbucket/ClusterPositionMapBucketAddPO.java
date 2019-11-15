package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPositionMapBucketAddPO extends PageOperationRecord {
  private int  recordPageIndex;
  private int  recordPosition;
  private byte status;

  public ClusterPositionMapBucketAddPO() {
  }

  public ClusterPositionMapBucketAddPO(int recordPageIndex, int recordPosition, byte status) {
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
    this.status = status;
  }

  public int getRecordPageIndex() {
    return recordPageIndex;
  }

  public int getRecordPosition() {
    return recordPosition;
  }

  public byte getStatus() {
    return status;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    bucket.add(recordPageIndex, recordPosition, status);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    bucket.truncateLastEntry();
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_ADD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(status);
    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    status = buffer.get();
    recordPageIndex = buffer.getInt();
    recordPosition = buffer.getInt();
  }
}
