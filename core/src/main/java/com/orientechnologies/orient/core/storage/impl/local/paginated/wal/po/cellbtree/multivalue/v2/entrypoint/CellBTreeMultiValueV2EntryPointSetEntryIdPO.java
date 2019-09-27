package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.entrypoint;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2EntryPoint;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV2EntryPointSetEntryIdPO extends PageOperationRecord {
  private long entryId;
  private long prevEntryId;

  public CellBTreeMultiValueV2EntryPointSetEntryIdPO() {
  }

  public CellBTreeMultiValueV2EntryPointSetEntryIdPO(long entryId, long prevEntryId) {
    this.entryId = entryId;
    this.prevEntryId = prevEntryId;
  }

  public long getEntryId() {
    return entryId;
  }

  public long getPrevEntryId() {
    return prevEntryId;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(cacheEntry);
    bucket.setEntryId(entryId);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2EntryPoint bucket = new CellBTreeMultiValueV2EntryPoint(cacheEntry);
    bucket.setEntryId(prevEntryId);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_ENTRY_POINT_MULTI_VALUE_V2_SET_ENTRY_ID_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(entryId);
    buffer.putLong(prevEntryId);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    entryId = buffer.getLong();
    prevEntryId = buffer.getLong();
  }
}
