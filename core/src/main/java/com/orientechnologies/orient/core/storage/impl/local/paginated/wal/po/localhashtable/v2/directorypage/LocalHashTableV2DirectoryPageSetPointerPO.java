package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directorypage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.DirectoryPageV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2DirectoryPageSetPointerPO extends PageOperationRecord {
  private int  localNodeIndex;
  private int  index;
  private long pointer;
  private long pastPointer;

  public LocalHashTableV2DirectoryPageSetPointerPO() {
  }

  public LocalHashTableV2DirectoryPageSetPointerPO(int localNodeIndex, int index, long pointer, long pastPointer) {
    this.localNodeIndex = localNodeIndex;
    this.index = index;
    this.pointer = pointer;
    this.pastPointer = pastPointer;
  }

  public int getLocalNodeIndex() {
    return localNodeIndex;
  }

  public int getIndex() {
    return index;
  }

  public long getPointer() {
    return pointer;
  }

  public long getPastPointer() {
    return pastPointer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);
    page.setPointer(localNodeIndex, index, pointer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);
    page.setPointer(localNodeIndex, index, pastPointer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_POINTER_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(localNodeIndex);
    buffer.putInt(index);

    buffer.putLong(pointer);
    buffer.putLong(pastPointer);

  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    localNodeIndex = buffer.getInt();
    index = buffer.getInt();

    pointer = buffer.getLong();
    pastPointer = buffer.getLong();
  }
}
