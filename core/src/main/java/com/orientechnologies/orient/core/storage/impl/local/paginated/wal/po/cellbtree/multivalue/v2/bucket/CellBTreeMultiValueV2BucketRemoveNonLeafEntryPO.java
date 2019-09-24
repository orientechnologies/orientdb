package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2Bucket;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO extends PageOperationRecord {
  private int    index;
  private byte[] key;
  private int    left;
  private int    right;
  private int    prevChild;

  public CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO() {
  }

  public CellBTreeMultiValueV2BucketRemoveNonLeafEntryPO(int index, byte[] key, int left, int right, int prevChild) {
    this.index = index;
    this.key = key;
    this.left = left;
    this.right = right;
    this.prevChild = prevChild;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getKey() {
    return key;
  }

  public int getLeft() {
    return left;
  }

  public int getRight() {
    return right;
  }

  public int getPrevChild() {
    return prevChild;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2Bucket bucket = new CellBTreeMultiValueV2Bucket(cacheEntry);
    bucket.removeNonLeafEntry(index, key, prevChild);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2Bucket bucket = new CellBTreeMultiValueV2Bucket(cacheEntry);
    final boolean result = bucket.addNonLeafEntry(index, key, left, right, true);
    if (!result) {
      throw new IllegalStateException("Can not undo remove leaf entry operation");
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_NON_LEAF_ENTRY_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 5 * OIntegerSerializer.INT_SIZE + key.length;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);
    buffer.putInt(left);
    buffer.putInt(right);
    buffer.putInt(prevChild);

    buffer.putInt(key.length);
    buffer.put(key);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();
    left = buffer.getInt();
    right = buffer.getInt();
    prevChild = buffer.getInt();

    final int keyLen = buffer.getInt();
    key = new byte[keyLen];
    buffer.get(key);
  }
}
