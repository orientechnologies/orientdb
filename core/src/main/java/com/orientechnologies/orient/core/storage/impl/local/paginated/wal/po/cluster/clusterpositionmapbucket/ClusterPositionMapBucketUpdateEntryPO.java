package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.clusterpositionmapbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.OClusterPositionMapBucket;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;

import java.nio.ByteBuffer;

public final class ClusterPositionMapBucketUpdateEntryPO extends PageOperationRecord {
  private int index;

  private byte oldRecordStatus;
  private int  oldRecordPageIndex;
  private int  oldRecordPosition;

  private byte recordStatus;
  private int  recordPageIndex;
  private int  recordPosition;

  public ClusterPositionMapBucketUpdateEntryPO() {
  }

  public ClusterPositionMapBucketUpdateEntryPO(int index, byte oldRecordStatus, int oldRecordPageIndex, int oldRecordPosition,
      byte recordStatus, int recordPageIndex, int recordPosition) {
    this.index = index;

    this.oldRecordStatus = oldRecordStatus;
    this.oldRecordPageIndex = oldRecordPageIndex;
    this.oldRecordPosition = oldRecordPosition;

    this.recordStatus = recordStatus;
    this.recordPageIndex = recordPageIndex;
    this.recordPosition = recordPosition;
  }

  public int getIndex() {
    return index;
  }

  public byte getOldRecordStatus() {
    return oldRecordStatus;
  }

  public int getOldRecordPageIndex() {
    return oldRecordPageIndex;
  }

  public int getOldRecordPosition() {
    return oldRecordPosition;
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
    bucket.updateEntry(index, recordPageIndex, recordPosition, recordStatus);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OClusterPositionMapBucket bucket = new OClusterPositionMapBucket(cacheEntry);
    bucket.updateEntry(index, oldRecordPageIndex, oldRecordPosition, oldRecordStatus);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CLUSTER_POSITION_MAP_UPDATE_ENTRY_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + 2 * (OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE);
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);

    buffer.put(oldRecordStatus);
    buffer.putInt(oldRecordPageIndex);
    buffer.putInt(oldRecordPosition);

    buffer.put(recordStatus);
    buffer.putInt(recordPageIndex);
    buffer.putInt(recordPosition);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();

    oldRecordStatus = buffer.get();
    oldRecordPageIndex = buffer.getInt();
    oldRecordPosition = buffer.getInt();

    recordStatus = buffer.get();
    recordPageIndex = buffer.getInt();
    recordPosition = buffer.getInt();
  }
}
