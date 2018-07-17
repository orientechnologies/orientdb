package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OPageOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class OSBTreeBonsaiBucketShrinkOperation extends OPageOperation {
  private List<byte[]> removedEntries;
  private int          pageOffset;

  public OSBTreeBonsaiBucketShrinkOperation() {
  }

  public OSBTreeBonsaiBucketShrinkOperation(OLogSequenceNumber pageLSN, long fileId, long pageIndex, int pageOffset,
      List<byte[]> removedEntries) {
    super(pageLSN, fileId, pageIndex);
    this.removedEntries = removedEntries;
    this.pageOffset = pageOffset;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_BONSAI_BUCKET_SHRINK_OPERATION;
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();

    size += 2 * OIntegerSerializer.INT_SIZE;

    for (byte[] removedOperation : removedEntries) {
      size += OIntegerSerializer.INT_SIZE + removedOperation.length;
    }

    return size;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(pageOffset, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(removedEntries.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (byte[] removedOperation : removedEntries) {
      OIntegerSerializer.INSTANCE.serializeNative(removedOperation.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(removedOperation, 0, content, offset, removedOperation.length);
      offset += removedOperation.length;
    }

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pageOffset);
    buffer.putInt(removedEntries.size());

    for (byte[] removedOperation : removedEntries) {
      buffer.putInt(removedOperation.length);
      buffer.put(removedOperation);
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    pageOffset = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    int amountRemoved = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    removedEntries = new ArrayList<>(amountRemoved);
    for (int i = 0; i < amountRemoved; i++) {
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

  public int getPageOffset() {
    return pageOffset;
  }
}
