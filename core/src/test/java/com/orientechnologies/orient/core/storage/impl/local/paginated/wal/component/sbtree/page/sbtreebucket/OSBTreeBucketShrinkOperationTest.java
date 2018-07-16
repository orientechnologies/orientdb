package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OSBTreeBucketShrinkOperationTest {
  @Test
  public void testArraySerialization() {
    final byte[] entryOne = new byte[12];
    final byte[] entryTwo = new byte[35];

    final Random random = new Random();

    random.nextBytes(entryOne);
    random.nextBytes(entryTwo);

    final List<byte[]> removedEntries = new ArrayList<>();
    removedEntries.add(entryOne);
    removedEntries.add(entryTwo);

    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);
    long fileId = 34;
    long pageIndex = 56;

    OSBTreeBucketShrinkOperation operation = new OSBTreeBucketShrinkOperation(lsn, fileId, pageIndex, removedEntries);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OSBTreeBucketShrinkOperation restored = new OSBTreeBucketShrinkOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(removedEntries.size(), restored.getRemovedEntries().size());
    for (int i = 0; i < removedEntries.size(); i++) {
      byte[] valueOne = removedEntries.get(i);
      byte[] valueTwo = restored.getRemovedEntries().get(i);

      Assert.assertArrayEquals(valueOne, valueTwo);
    }
  }

  @Test
  public void testBufferSerialization() {
    final byte[] entryOne = new byte[12];
    final byte[] entryTwo = new byte[35];

    final Random random = new Random();

    random.nextBytes(entryOne);
    random.nextBytes(entryTwo);

    final List<byte[]> removedEntries = new ArrayList<>();
    removedEntries.add(entryOne);
    removedEntries.add(entryTwo);

    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);
    long fileId = 34;
    long pageIndex = 56;

    OSBTreeBucketShrinkOperation operation = new OSBTreeBucketShrinkOperation(lsn, fileId, pageIndex, removedEntries);
    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketShrinkOperation restored = new OSBTreeBucketShrinkOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(removedEntries.size(), restored.getRemovedEntries().size());
    for (int i = 0; i < removedEntries.size(); i++) {
      byte[] valueOne = removedEntries.get(i);
      byte[] valueTwo = restored.getRemovedEntries().get(i);

      Assert.assertArrayEquals(valueOne, valueTwo);
    }
  }
}
