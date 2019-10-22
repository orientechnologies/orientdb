package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.DirectoryFirstPageV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO extends PageOperationRecord {
  private int  localNodeIndex;
  private byte maxLeftChildDepth;
  private byte pastMaxLeftChildDepth;

  public LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO() {
  }

  public LocalHashTableV2DirectoryFirstPageSetMaxLeftChildDepthPO(int localNodeIndex, byte maxLeftChildDepth,
      byte pastMaxLeftChildDepth) {
    this.localNodeIndex = localNodeIndex;
    this.maxLeftChildDepth = maxLeftChildDepth;
    this.pastMaxLeftChildDepth = pastMaxLeftChildDepth;
  }

  public int getLocalNodeIndex() {
    return localNodeIndex;
  }

  public byte getMaxLeftChildDepth() {
    return maxLeftChildDepth;
  }

  public byte getPastMaxLeftChildDepth() {
    return pastMaxLeftChildDepth;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setMaxLeftChildDepth(localNodeIndex, maxLeftChildDepth);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setMaxLeftChildDepth(localNodeIndex, pastMaxLeftChildDepth);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_MAX_LEFT_CHILDREN_DEPTH_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + 2 * OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(localNodeIndex);

    buffer.put(maxLeftChildDepth);
    buffer.put(pastMaxLeftChildDepth);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    localNodeIndex = buffer.getInt();

    maxLeftChildDepth = buffer.get();
    pastMaxLeftChildDepth = buffer.get();
  }
}
