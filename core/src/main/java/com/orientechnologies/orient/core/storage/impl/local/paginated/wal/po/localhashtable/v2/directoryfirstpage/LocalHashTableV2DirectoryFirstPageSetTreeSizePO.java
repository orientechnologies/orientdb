package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.DirectoryFirstPageV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2DirectoryFirstPageSetTreeSizePO extends PageOperationRecord {
  private int size;
  private int pastSize;

  public LocalHashTableV2DirectoryFirstPageSetTreeSizePO() {
  }

  public LocalHashTableV2DirectoryFirstPageSetTreeSizePO(int size, int pastSize) {
    this.size = size;
    this.pastSize = pastSize;
  }

  public int getSize() {
    return size;
  }

  public int getPastSize() {
    return pastSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setTreeSize(size);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setTreeSize(pastSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_TREE_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(size);
    buffer.putInt(pastSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    size = buffer.getInt();
    pastSize = buffer.getInt();
  }
}
