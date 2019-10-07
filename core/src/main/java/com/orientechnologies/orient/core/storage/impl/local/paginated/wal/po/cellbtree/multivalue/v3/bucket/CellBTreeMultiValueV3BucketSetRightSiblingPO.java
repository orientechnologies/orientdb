package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.bucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3Bucket;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV3BucketSetRightSiblingPO extends PageOperationRecord {
  private long sibling;
  private long prevSibling;

  public CellBTreeMultiValueV3BucketSetRightSiblingPO() {
  }

  public CellBTreeMultiValueV3BucketSetRightSiblingPO(long sibling, long prevSibling) {
    this.sibling = sibling;
    this.prevSibling = prevSibling;
  }

  public long getSibling() {
    return sibling;
  }

  public long getPrevSibling() {
    return prevSibling;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3Bucket bucket = new CellBTreeMultiValueV3Bucket(cacheEntry);
    bucket.setRightSibling(sibling);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3Bucket bucket = new CellBTreeMultiValueV3Bucket(cacheEntry);
    bucket.setRightSibling(prevSibling);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_MULTI_VALUE_V3_SET_RIGHT_SIBLING_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(sibling);
    buffer.putLong(prevSibling);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    sibling = buffer.getLong();
    prevSibling = buffer.getLong();
  }
}
