package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.entrypoint;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeSingleValueEntryPointV1;

import java.nio.ByteBuffer;

public final class CellBTreeEntryPointSingleValueV1SetPagesSizePO extends PageOperationRecord {
  private int prevPagesSize;
  private int pagesSize;

  public CellBTreeEntryPointSingleValueV1SetPagesSizePO() {
  }

  public CellBTreeEntryPointSingleValueV1SetPagesSizePO(int prevPagesSize, int pagesSize) {
    this.prevPagesSize = prevPagesSize;
    this.pagesSize = pagesSize;
  }

  public int getPrevPagesSize() {
    return prevPagesSize;
  }

  public int getPagesSize() {
    return pagesSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueEntryPointV1 bucket = new CellBTreeSingleValueEntryPointV1(cacheEntry);
    bucket.setPagesSize(pagesSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueEntryPointV1 bucket = new CellBTreeSingleValueEntryPointV1(cacheEntry);
    bucket.setPagesSize(prevPagesSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V1_SET_PAGES_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(prevPagesSize);
    buffer.putInt(pagesSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    prevPagesSize = buffer.getInt();
    pagesSize = buffer.getInt();
  }
}
