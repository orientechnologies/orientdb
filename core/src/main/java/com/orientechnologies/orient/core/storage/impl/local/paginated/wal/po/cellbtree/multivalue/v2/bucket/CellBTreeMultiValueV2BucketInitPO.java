package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2Bucket;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV2BucketInitPO extends PageOperationRecord {
  private boolean isLeaf;

  public CellBTreeMultiValueV2BucketInitPO() {
  }

  public CellBTreeMultiValueV2BucketInitPO(boolean isLeaf) {
    this.isLeaf = isLeaf;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2Bucket bucket = new CellBTreeMultiValueV2Bucket(cacheEntry);
    bucket.init(isLeaf);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_MULTI_VALUE_V2_INIT_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(isLeaf ? 1 : (byte) 0);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    isLeaf = buffer.get() > 0;
  }
}
