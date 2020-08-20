package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.version.v0.paginatedversionstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.versionmap.OPaginatedVersionStateV0;

import java.nio.ByteBuffer;

public final class PaginatedVersionStateV0SetFreeListPagePO extends PageOperationRecord {
  private int index;

  private int oldPageIndex;
  private int newPageIndex;

  public PaginatedVersionStateV0SetFreeListPagePO() {}

  public PaginatedVersionStateV0SetFreeListPagePO(int index, int oldPageIndex, int newPageIndex) {
    this.index = index;
    this.oldPageIndex = oldPageIndex;
    this.newPageIndex = newPageIndex;
  }

  public int getIndex() {
    return index;
  }

  public int getOldPageIndex() {
    return oldPageIndex;
  }

  public int getNewPageIndex() {
    return newPageIndex;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OPaginatedVersionStateV0 paginatedVersionStateV0 =
        new OPaginatedVersionStateV0(cacheEntry);
    paginatedVersionStateV0.setFreeListPage(index, newPageIndex);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedVersionStateV0 paginatedVersionStateV0 =
        new OPaginatedVersionStateV0(cacheEntry);
    paginatedVersionStateV0.setFreeListPage(index, oldPageIndex);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_VERSION_STATE_V0_SET_FILE_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 3 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);

    buffer.putInt(oldPageIndex);
    buffer.putInt(newPageIndex);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();

    oldPageIndex = buffer.getInt();
    newPageIndex = buffer.getInt();
  }
}
