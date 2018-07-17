package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class OSBTreeBonsaiBucketShrinkOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 45;
    long pageIndex = 56;

    int pageOffset = 76;

    byte[] entryOne = new byte[34];
    byte[] entryTwo = new byte[23];

    List<byte[]> removedEntries = new ArrayList<>();
    removedEntries.add(entryOne);
    removedEntries.add(entryTwo);

    OSBTreeBonsaiBucketShrinkOperation operation = new OSBTreeBonsaiBucketShrinkOperation(lsn, fileId, pageIndex, pageOffset,
        removedEntries);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketShrinkOperation restored = new OSBTreeBonsaiBucketShrinkOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(removedEntries.size(), restored.getRemovedEntries().size());

    for (int i = 0; i < restored.getRemovedEntries().size(); i++) {
      Assert.assertArrayEquals(removedEntries.get(i), restored.getRemovedEntries().get(i));
    }
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 45;
    long pageIndex = 56;

    int pageOffset = 76;

    byte[] entryOne = new byte[34];
    byte[] entryTwo = new byte[23];

    List<byte[]> removedEntries = new ArrayList<>();
    removedEntries.add(entryOne);
    removedEntries.add(entryTwo);

    OSBTreeBonsaiBucketShrinkOperation operation = new OSBTreeBonsaiBucketShrinkOperation(lsn, fileId, pageIndex, pageOffset,
        removedEntries);

    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketShrinkOperation restored = new OSBTreeBonsaiBucketShrinkOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(removedEntries.size(), restored.getRemovedEntries().size());

    for (int i = 0; i < restored.getRemovedEntries().size(); i++) {
      Assert.assertArrayEquals(removedEntries.get(i), restored.getRemovedEntries().get(i));
    }
  }
}
