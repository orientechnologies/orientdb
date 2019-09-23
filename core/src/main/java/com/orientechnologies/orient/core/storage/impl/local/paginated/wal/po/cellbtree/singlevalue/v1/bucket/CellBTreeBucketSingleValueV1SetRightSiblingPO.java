package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeBucketSingleValueV1;

import java.nio.ByteBuffer;

public final class CellBTreeBucketSingleValueV1SetRightSiblingPO extends PageOperationRecord {
  private int prevRightSibling;
  private int rightSibling;

  public CellBTreeBucketSingleValueV1SetRightSiblingPO() {
  }

  public CellBTreeBucketSingleValueV1SetRightSiblingPO(int prevRightSibling, int rightSibling) {
    this.prevRightSibling = prevRightSibling;
    this.rightSibling = rightSibling;
  }

  public int getPrevRightSibling() {
    return prevRightSibling;
  }

  public int getRightSibling() {
    return rightSibling;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeBucketSingleValueV1 bucket = new CellBTreeBucketSingleValueV1(cacheEntry);
    bucket.setRightSibling(rightSibling);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeBucketSingleValueV1 bucket = new CellBTreeBucketSingleValueV1(cacheEntry);
    bucket.setRightSibling(prevRightSibling);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V1_SET_RIGHT_SIBLING_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(prevRightSibling);
    buffer.putInt(rightSibling);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    prevRightSibling = buffer.getInt();
    rightSibling = buffer.getInt();
  }
}
