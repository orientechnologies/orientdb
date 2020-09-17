package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.version.v0.paginatedversionstate;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.versionmap.OPaginatedVersionStateV0;
import java.nio.ByteBuffer;

public final class PaginatedVersionStateV0SetRecordsSizePO extends PageOperationRecord {
  private int oldRecordsSize;
  private int newRecordsSize;

  public PaginatedVersionStateV0SetRecordsSizePO() {}

  public PaginatedVersionStateV0SetRecordsSizePO(int oldRecordsSize, int newRecordsSize) {
    this.oldRecordsSize = oldRecordsSize;
    this.newRecordsSize = newRecordsSize;
  }

  public long getOldRecordsSize() {
    return oldRecordsSize;
  }

  public long getNewRecordsSize() {
    return newRecordsSize;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final OPaginatedVersionStateV0 paginatedVersionStateV0 =
        new OPaginatedVersionStateV0(cacheEntry);
    paginatedVersionStateV0.setRecordsSize(newRecordsSize);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final OPaginatedVersionStateV0 paginatedVersionStateV0 =
        new OPaginatedVersionStateV0(cacheEntry);
    paginatedVersionStateV0.setRecordsSize(oldRecordsSize);
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

    buffer.putInt(oldRecordsSize);
    buffer.putInt(newRecordsSize);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldRecordsSize = buffer.getInt();
    newRecordsSize = buffer.getInt();
  }
}
