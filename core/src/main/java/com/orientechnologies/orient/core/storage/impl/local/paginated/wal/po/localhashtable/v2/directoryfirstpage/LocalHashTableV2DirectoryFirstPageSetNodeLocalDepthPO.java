package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.directoryfirstpage;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.DirectoryFirstPageV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2DirectoryFirstPageSetNodeLocalDepthPO extends PageOperationRecord {
  private int  localNodeIndex;
  private byte nodeLocalDepth;
  private byte pastNodeLocalDepth;

  public LocalHashTableV2DirectoryFirstPageSetNodeLocalDepthPO() {
  }

  public LocalHashTableV2DirectoryFirstPageSetNodeLocalDepthPO(int localNodeIndex, byte nodeLocalDepth, byte pastNodeLocalDepth) {
    this.localNodeIndex = localNodeIndex;
    this.nodeLocalDepth = nodeLocalDepth;
    this.pastNodeLocalDepth = pastNodeLocalDepth;
  }

  public int getLocalNodeIndex() {
    return localNodeIndex;
  }

  public byte getNodeLocalDepth() {
    return nodeLocalDepth;
  }

  public byte getPastNodeLocalDepth() {
    return pastNodeLocalDepth;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setNodeLocalDepth(localNodeIndex, nodeLocalDepth);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final DirectoryFirstPageV2 page = new DirectoryFirstPageV2(cacheEntry);
    page.setNodeLocalDepth(localNodeIndex, pastNodeLocalDepth);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_DIRECTORY_FIRST_PAGE_SET_NODE_LOCAL_DEPTH_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OIntegerSerializer.INT_SIZE + 2 * OByteSerializer.BYTE_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(localNodeIndex);

    buffer.put(nodeLocalDepth);
    buffer.put(pastNodeLocalDepth);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    localNodeIndex = buffer.getInt();

    nodeLocalDepth = buffer.get();
    pastNodeLocalDepth = buffer.get();
  }
}
