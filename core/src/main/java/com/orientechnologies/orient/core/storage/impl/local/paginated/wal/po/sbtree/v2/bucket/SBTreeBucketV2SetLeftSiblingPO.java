package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.sbtree.v2.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.local.v2.OSBTreeBucketV2;

import java.nio.ByteBuffer;

public final class SBTreeBucketV2SetLeftSiblingPO extends PageOperationRecord {
  private int prevLeftSibling;
  private int leftSibling;

  public SBTreeBucketV2SetLeftSiblingPO() {
  }

  public SBTreeBucketV2SetLeftSiblingPO(int prevLeftSibling, int leftSibling) {
    this.prevLeftSibling = prevLeftSibling;
    this.leftSibling = leftSibling;
  }

  public int getPrevLeftSibling() {
    return prevLeftSibling;
  }

  public int getLeftSibling() {
    return leftSibling;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    bucket.setLeftSibling(leftSibling);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OSBTreeBucketV2 bucket = new OSBTreeBucketV2(cacheEntry);
    bucket.setLeftSibling(prevLeftSibling);
  }

  @Override
  public int getId() {
    return WALRecordTypes.SBTREE_BUCKET_V2_SET_LEFT_SIBLING_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(prevLeftSibling);
    buffer.putInt(leftSibling);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    prevLeftSibling = buffer.getInt();
    leftSibling = buffer.getInt();
  }
}
