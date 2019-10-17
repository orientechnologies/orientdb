package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directorypage;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.DirectoryPageV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2DirectoryPageSetMaxRightChildDepthPO extends PageOperationRecord {
  private int  localNodeIndex;
  private byte maxRightChildDepth;
  private byte pastMaxRightChildDepth;

  public LocalHashTableV2DirectoryPageSetMaxRightChildDepthPO() {
  }

  public LocalHashTableV2DirectoryPageSetMaxRightChildDepthPO(int localNodeIndex, byte maxRightChildDepth,
      byte pastMaxRightChildDepth) {
    this.localNodeIndex = localNodeIndex;
    this.maxRightChildDepth = maxRightChildDepth;
    this.pastMaxRightChildDepth = pastMaxRightChildDepth;
  }

  public int getLocalNodeIndex() {
    return localNodeIndex;
  }

  public byte getMaxRightChildDepth() {
    return maxRightChildDepth;
  }

  public byte getPastMaxRightChildDepth() {
    return pastMaxRightChildDepth;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);
    page.setMaxRightChildDepth(localNodeIndex, maxRightChildDepth);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final DirectoryPageV2 page = new DirectoryPageV2(cacheEntry);
    page.setMaxRightChildDepth(localNodeIndex, pastMaxRightChildDepth);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_DIRECTORY_PAGE_SET_MAX_RIGHT_CHILDREN_DEPTH_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + 2 * OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(localNodeIndex);

    buffer.put(maxRightChildDepth);
    buffer.put(pastMaxRightChildDepth);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    localNodeIndex = buffer.getInt();

    maxRightChildDepth = buffer.get();
    pastMaxRightChildDepth = buffer.get();
  }
}
