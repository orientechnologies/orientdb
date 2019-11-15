package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.entrypoint;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueEntryPointV3;

import java.nio.ByteBuffer;

public final class CellBTreeEntryPointSingleValueV3SetTreeSizePO extends PageOperationRecord {
  private long prevTreeSize;
  private long treeSize;

  public CellBTreeEntryPointSingleValueV3SetTreeSizePO() {
  }

  public CellBTreeEntryPointSingleValueV3SetTreeSizePO(long prevTreeSize, long treeSize) {
    this.prevTreeSize = prevTreeSize;
    this.treeSize = treeSize;
  }

  public long getPrevTreeSize() {
    return prevTreeSize;
  }

  public long getTreeSize() {
    return treeSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueEntryPointV3 bucket = new CellBTreeSingleValueEntryPointV3(cacheEntry);
    bucket.setTreeSize(treeSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueEntryPointV3 bucket = new CellBTreeSingleValueEntryPointV3(cacheEntry);
    bucket.setTreeSize(prevTreeSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_SINGLE_VALUE_V3_SET_TREE_SIZE_PO;
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
