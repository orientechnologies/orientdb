package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.version.v0.paginatedversionstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.versionmap.OPaginatedVersionStateV0;

import java.nio.ByteBuffer;

public final class PaginatedVersionStateV0SetFileSizePO extends PageOperationRecord {
  private int oldFileSize;
  private int newFileSize;

  public PaginatedVersionStateV0SetFileSizePO() {}

  public PaginatedVersionStateV0SetFileSizePO(int oldFileSize, int newFileSize) {
    this.oldFileSize = oldFileSize;
    this.newFileSize = newFileSize;
  }

  public int getOldFileSize() {
    return oldFileSize;
  }

  public int getNewFileSize() {
    return newFileSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OPaginatedVersionStateV0 paginatedVersionStateV0 =
        new OPaginatedVersionStateV0(cacheEntry);
    paginatedVersionStateV0.setFileSize(newFileSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedVersionStateV0 paginatedVersionStateV0 =
        new OPaginatedVersionStateV0(cacheEntry);
    paginatedVersionStateV0.setFileSize(oldFileSize);
  }

  @Override
  public int getId() {
    return WALRecordTypes.PAGINATED_VERSION_STATE_V0_SET_FILE_SIZE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(oldFileSize);
    buffer.putInt(newFileSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldFileSize = buffer.getInt();
    newFileSize = buffer.getInt();
  }
}
