package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.bucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueBucketV3;

import java.nio.ByteBuffer;

public final class CellBTreeBucketSingleValueV3InitPO extends PageOperationRecord {
  private boolean isLeaf;

  public CellBTreeBucketSingleValueV3InitPO() {
  }

  public CellBTreeBucketSingleValueV3InitPO(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueBucketV3 bucket = new CellBTreeSingleValueBucketV3(cacheEntry);
    bucket.init(isLeaf);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V3_INIT_PO;
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
