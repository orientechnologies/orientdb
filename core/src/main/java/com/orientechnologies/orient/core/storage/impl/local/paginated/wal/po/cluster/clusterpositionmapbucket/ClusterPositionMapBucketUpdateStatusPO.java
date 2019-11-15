package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPositionMapBucketUpdateStatusPO extends PageOperationRecord {
  private int index;

  private byte recordOldStatus;
  private byte recordStatus;

  public ClusterPositionMapBucketUpdateStatusPO() {
  }

  public ClusterPositionMapBucketUpdateStatusPO(int index, byte recordOldStatus, byte recordStatus) {
    this.index = index;
    this.recordOldStatus = recordOldStatus;
    this.recordStatus = recordStatus;
  }

  public int getIndex() {
    return index;
  }

  public byte getRecordOldStatus() {
    return recordOldStatus;
  }

  public byte getRecordStatus() {
    return recordStatus;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    bucket.updateStatus(index, recordStatus);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    bucket.updateStatus(index, recordOldStatus);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UPDATE_STATUS_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + 2 * OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);

    buffer.put(recordOldStatus);
    buffer.put(recordStatus);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();

    recordOldStatus = buffer.get();
    recordStatus = buffer.get();
  }
}
