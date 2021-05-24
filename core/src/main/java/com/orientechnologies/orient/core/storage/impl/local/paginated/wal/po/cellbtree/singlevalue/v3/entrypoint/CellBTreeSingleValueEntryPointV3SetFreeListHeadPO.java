package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueEntryPointV3;
import java.nio.ByteBuffer;

public final class CellBTreeSingleValueEntryPointV3SetFreeListHeadPO extends PageOperationRecord {
  private int freeListHead;
  private int prevFreeListHead;

  public CellBTreeSingleValueEntryPointV3SetFreeListHeadPO() {}

  public CellBTreeSingleValueEntryPointV3SetFreeListHeadPO(int freeListHead, int prevFreeListHead) {
    this.freeListHead = freeListHead;
    this.prevFreeListHead = prevFreeListHead;
  }

  public int getFreeListHead() {
    return freeListHead;
  }

  public int getPrevFreeListHead() {
    return prevFreeListHead;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueEntryPointV3<?> bucket =
        new CellBTreeSingleValueEntryPointV3<>(cacheEntry);
    bucket.setFreeListHead(freeListHead);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueEntryPointV3<?> bucket =
        new CellBTreeSingleValueEntryPointV3<>(cacheEntry);
    bucket.setFreeListHead(prevFreeListHead);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_FREE_LIST_HEAD_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(freeListHead);
    buffer.putInt(prevFreeListHead);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    freeListHead = buffer.getInt();
    prevFreeListHead = buffer.getInt();
  }
}
