package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public class ClusterPositionMapBucketAddPO extends PageOperationRecord {
  private int recordPageIndex;
  private int recordPosition;

  public ClusterPositionMapBucketAddPO() {
  }

  public ClusterPositionMapBucketAddPO(int recordPageIndex, int recordPosition) {
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
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
    bucket.add(recordPageIndex, recordPosition);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    final int size = bucket.getSize();
    bucket.remove(size - 1);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_ADD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    recordPageIndex = buffer.getInt();
    recordPosition = buffer.getInt();
  }
}
