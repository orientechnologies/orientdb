package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeBucketV1;

import java.nio.ByteBuffer;

public final class SBTreeBucketV1SetTreeSizePO extends PageOperationRecord {
  private long prevTreeSize;
  private long treeSize;

  public SBTreeBucketV1SetTreeSizePO() {
  }

  public SBTreeBucketV1SetTreeSizePO(long prevTreeSize, long treeSize) {
    this.prevTreeSize = prevTreeSize;
    this.treeSize = treeSize;
  }

  public long getPrevTreeSize() {
    return prevTreeSize;
  }

  public long getTreeSize() {
    return treeSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV1 bucket = new OSBTreeBucketV1(cacheEntry);
    bucket.setTreeSize(treeSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV1 bucket = new OSBTreeBucketV1(cacheEntry);
    bucket.setTreeSize(prevTreeSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V1_SET_TREE_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(prevTreeSize);
    buffer.putLong(treeSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    prevTreeSize = buffer.getLong();
    treeSize = buffer.getLong();
  }
}
