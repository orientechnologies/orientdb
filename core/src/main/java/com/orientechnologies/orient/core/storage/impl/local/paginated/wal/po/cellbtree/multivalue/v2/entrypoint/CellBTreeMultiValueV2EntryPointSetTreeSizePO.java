package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2EntryPoint;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV2EntryPointSetTreeSizePO extends PageOperationRecord {
  private long treeSize;
  private long prevTreeSize;

  public CellBTreeMultiValueV2EntryPointSetTreeSizePO() {
  }

  public CellBTreeMultiValueV2EntryPointSetTreeSizePO(long treeSize, long prevTreeSize) {
    this.treeSize = treeSize;
    this.prevTreeSize = prevTreeSize;
  }

  public long getTreeSize() {
    return treeSize;
  }

  public long getPrevTreeSize() {
    return prevTreeSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(cacheEntry);
    bucket.setTreeSize(treeSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(cacheEntry);
    bucket.setTreeSize(prevTreeSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_TREE_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(treeSize);
    buffer.putLong(prevTreeSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    treeSize = buffer.getLong();
    prevTreeSize = buffer.getLong();
  }
}
