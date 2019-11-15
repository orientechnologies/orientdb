package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.DirectoryFirstPageV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2DirectoryFirstPageSetTombstonePO extends PageOperationRecord {
  private int tombstone;
  private int pastTombstone;

  public LocalHashTableV2DirectoryFirstPageSetTombstonePO() {
  }

  public LocalHashTableV2DirectoryFirstPageSetTombstonePO(int tombstone, int pastTombstone) {
    this.tombstone = tombstone;
    this.pastTombstone = pastTombstone;
  }

  public int getTombstone() {
    return tombstone;
  }

  public int getPastTombstone() {
    return pastTombstone;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setTombstone(tombstone);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setTombstone(pastTombstone);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_TOMBSTONE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(tombstone);
    buffer.putInt(pastTombstone);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    tombstone = buffer.getInt();
    pastTombstone = buffer.getInt();
  }
}
