package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.entrypoint;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3EntryPoint;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV3EntryPointSetPagesSizePO extends PageOperationRecord {
  private int pagesSize;
  private int prevPagesSize;

  public CellBTreeMultiValueV3EntryPointSetPagesSizePO() {
  }

  public CellBTreeMultiValueV3EntryPointSetPagesSizePO(int pagesSize, int prevPagesSize) {
    this.pagesSize = pagesSize;
    this.prevPagesSize = prevPagesSize;
  }

  public long getPagesSize() {
    return pagesSize;
  }

  public long getPrevPagesSize() {
    return prevPagesSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3EntryPoint bucket = new CellBTreeMultiValueV3EntryPoint(cacheEntry);
    bucket.setPagesSize(pagesSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3EntryPoint bucket = new CellBTreeMultiValueV3EntryPoint(cacheEntry);
    bucket.setPagesSize(prevPagesSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V3_SET_PAGES_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(pagesSize);
    buffer.putInt(prevPagesSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    pagesSize = buffer.getInt();
    prevPagesSize = buffer.getInt();
  }
}
