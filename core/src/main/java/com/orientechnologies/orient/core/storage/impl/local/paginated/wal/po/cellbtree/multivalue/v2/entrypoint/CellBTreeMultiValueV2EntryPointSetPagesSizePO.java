package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2EntryPoint;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV2EntryPointSetPagesSizePO extends PageOperationRecord {
  private int pagesSize;
  private int prevPagesSize;

  public CellBTreeMultiValueV2EntryPointSetPagesSizePO() {
  }

  public CellBTreeMultiValueV2EntryPointSetPagesSizePO(int pagesSize, int prevPagesSize) {
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
    final CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(cacheEntry);
    bucket.setPagesSize(pagesSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(cacheEntry);
    bucket.setPagesSize(prevPagesSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_PAGES_SIZE_PO;
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
