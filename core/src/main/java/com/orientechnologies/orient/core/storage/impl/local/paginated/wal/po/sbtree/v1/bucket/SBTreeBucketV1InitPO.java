package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v1.bucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v1.OSBTreeBucketV1;

import java.nio.ByteBuffer;

public class SBTreeBucketV1InitPO extends PageOperationRecord {
  private boolean isLeaf;

  public SBTreeBucketV1InitPO() {
  }

  public SBTreeBucketV1InitPO(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  @Override
  public void redo(final OCacheEntry cacheEntry) {
    final OSBTreeBucketV1 bucket = new OSBTreeBucketV1(cacheEntry);
    bucket.init(isLeaf);
  }

  @Override
  public void undo(final OCacheEntry cacheEntry) {

  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V1_INIT_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(isLeaf ? (byte) 1 : 0);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    isLeaf = buffer.get() > 0;
  }
}
