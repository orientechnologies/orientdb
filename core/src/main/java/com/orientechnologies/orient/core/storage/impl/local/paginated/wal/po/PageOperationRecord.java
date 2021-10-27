package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitBodyRecord;
import java.nio.ByteBuffer;

public abstract class PageOperationRecord extends OOperationUnitBodyRecord {
  private long fileId;
  private int pageIndex;

  public PageOperationRecord() {}

  public abstract void redo(OCacheEntry cacheEntry);

  public abstract void undo(OCacheEntry cacheEntry);

  public void setFileId(long fileId) {
    this.fileId = fileId;
  }

  public void setPageIndex(int pageIndex) {
    this.pageIndex = pageIndex;
  }

  public long getFileId() {
    return fileId;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    buffer.putLong(fileId);
    buffer.putInt(pageIndex);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    fileId = buffer.getLong();
    pageIndex = buffer.getInt();
  }
}
