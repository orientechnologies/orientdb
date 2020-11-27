package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cluster.v2.freespacemap;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cluster.v2.FreeSpaceMapPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import java.nio.ByteBuffer;

public final class UpdateMaxFreeSpacePO extends PageOperationRecord {

  private int oldPageFreeSpace;
  private int newPageFreeSpace;
  private int pageIndex;

  public UpdateMaxFreeSpacePO() {}

  public UpdateMaxFreeSpacePO(int pageIndex, int oldPageFreeSpace, int newPageFreeSpace) {
    this.oldPageFreeSpace = oldPageFreeSpace;
    this.newPageFreeSpace = newPageFreeSpace;
    this.pageIndex = pageIndex;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
    page.updatePageMaxFreeSpace(pageIndex, newPageFreeSpace);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final FreeSpaceMapPage page = new FreeSpaceMapPage(cacheEntry);
    page.updatePageMaxFreeSpace(pageIndex, oldPageFreeSpace);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put((byte) oldPageFreeSpace);
    buffer.put((byte) newPageFreeSpace);

    buffer.putInt(pageIndex);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    oldPageFreeSpace = 0xFF & buffer.get();
    newPageFreeSpace = 0xFFb & buffer.get();

    pageIndex = buffer.getInt();
  }

  @Override
  public int getId() {
    return WALRecordTypes.FREE_SPACE_MAP_UPDATE;
  }
}
