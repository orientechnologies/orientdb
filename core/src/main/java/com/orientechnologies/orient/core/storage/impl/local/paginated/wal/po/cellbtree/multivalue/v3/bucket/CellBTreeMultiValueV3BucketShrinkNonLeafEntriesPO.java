package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.bucket;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3Bucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class CellBTreeMultiValueV3BucketShrinkNonLeafEntriesPO extends PageOperationRecord {
  private int                                            newSize;
  private List<CellBTreeMultiValueV3Bucket.NonLeafEntry> removedEntries;
  private OBinarySerializer                              keySerializer;

  public CellBTreeMultiValueV3BucketShrinkNonLeafEntriesPO() {
  }

  public CellBTreeMultiValueV3BucketShrinkNonLeafEntriesPO(final int newSize,
      final List<CellBTreeMultiValueV3Bucket.NonLeafEntry> removedEntries, final OBinarySerializer keySerializer) {
    this.newSize = newSize;
    this.removedEntries = removedEntries;
    this.keySerializer = keySerializer;
  }

  public int getNewSize() {
    return newSize;
  }

  public List<CellBTreeMultiValueV3Bucket.NonLeafEntry> getRemovedEntries() {
    return removedEntries;
  }

  public OBinarySerializer getKeySerializer() {
    return keySerializer;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3Bucket bucket = new CellBTreeMultiValueV3Bucket(cacheEntry);
    //noinspection unchecked
    bucket.shrink(newSize, keySerializer);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3Bucket bucket = new CellBTreeMultiValueV3Bucket(cacheEntry);
    //noinspection unchecked
    bucket.addAll(removedEntries, keySerializer);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_MULTI_VALUE_V3_SHRINK_NON_LEAF_ENTRIES_PO;
  }

  @Override
  public int serializedSize() {
    int size = 2 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;
    for (CellBTreeMultiValueV3Bucket.NonLeafEntry nonLeafEntry : removedEntries) {
      size += 3 * OIntegerSerializer.INT_SIZE + nonLeafEntry.key.length;
    }

    return super.serializedSize() + size;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(newSize);
    buffer.put(keySerializer.getId());

    buffer.putInt(removedEntries.size());

    for (final CellBTreeMultiValueV3Bucket.NonLeafEntry nonLeafEntry : removedEntries) {
      buffer.putInt(nonLeafEntry.leftChild);
      buffer.putInt(nonLeafEntry.rightChild);
      buffer.putInt(nonLeafEntry.key.length);
      buffer.put(nonLeafEntry.key);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    newSize = buffer.getInt();
    keySerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(buffer.get());

    final int entriesSize = buffer.getInt();
    removedEntries = new ArrayList<>(entriesSize);

    for (int i = 0; i < entriesSize; i++) {
      final int leftChild = buffer.getInt();
      final int rightChild = buffer.getInt();

      final int keyLen = buffer.getInt();
      final byte[] key = new byte[keyLen];
      buffer.get(key);

      final CellBTreeMultiValueV3Bucket.NonLeafEntry nonLeafEntry = new CellBTreeMultiValueV3Bucket.NonLeafEntry(key, leftChild,
          rightChild);
      removedEntries.add(nonLeafEntry);
    }
  }
}
