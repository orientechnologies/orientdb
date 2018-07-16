package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class OSBTreeBucketShrinkOperation extends OPageOperation {
  private List<byte[]> removedEntries;

  public OSBTreeBucketShrinkOperation() {
  }

  public OSBTreeBucketShrinkOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, List<byte[]> removedEntries) {
    super(pageLSN, fileId, pageIndex);
    this.removedEntries = removedEntries;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BUCKET_SHRINK_OPERATION;
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();

    size += OIntegerSerializer.INT_SIZE;
    for (byte[] entry : removedEntries) {
      size += entry.length + OIntegerSerializer.INT_SIZE;
    }

    return size;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(removedEntries.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (byte[] entry : removedEntries) {
      OIntegerSerializer.INSTANCE.serializeNative(entry.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(entry, 0, content, offset, entry.length);
      offset += entry.length;
    }

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(removedEntries.size());

    for (byte[] entry : removedEntries) {
      buffer.putInt(entry.length);
      buffer.put(entry);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int entriesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    removedEntries = new ArrayList<>(entriesSize);
    for (int i = 0; i < entriesSize; i++) {
      final int entrySize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final byte[] entry = new byte[entrySize];
      System.arraycopy(content, offset, entry, 0, entrySize);
      offset += entrySize;

      removedEntries.add(entry);
    }

    return offset;
  }

  public List<byte[]> getRemovedEntries() {
    return removedEntries;
  }
}
