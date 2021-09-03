package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueBucketV3;
import java.nio.ByteBuffer;

public final class CellBTreeBucketSingleValueV3SetNextFreeListPagePO extends PageOperationRecord {
  private int nextFreeListPage;
  private int prevNextFreeListPage;

  public CellBTreeBucketSingleValueV3SetNextFreeListPagePO() {}

  public CellBTreeBucketSingleValueV3SetNextFreeListPagePO(
      int nextFreeListPage, int prevNextFreeListPage) {
    this.nextFreeListPage = nextFreeListPage;
    this.prevNextFreeListPage = prevNextFreeListPage;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueBucketV3<?> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
    bucket.setNextFreeListPage(nextFreeListPage);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueBucketV3<?> bucket = new CellBTreeSingleValueBucketV3<>(cacheEntry);
    bucket.setNextFreeListPage(prevNextFreeListPage);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_SINGLE_VALUE_V3_SET_NEXT_FREE_LIST_PAGE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);
    buffer.putInt(nextFreeListPage);
    buffer.putInt(prevNextFreeListPage);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    nextFreeListPage = buffer.getInt();
    prevNextFreeListPage = buffer.getInt();
  }
}
