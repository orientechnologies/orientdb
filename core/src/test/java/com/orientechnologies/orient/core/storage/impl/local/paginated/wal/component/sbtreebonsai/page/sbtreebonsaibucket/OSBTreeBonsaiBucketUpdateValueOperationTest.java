package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeBonsaiBucketUpdateValueOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(34, 55);
    long fileId = 90;
    long pageIndex = 12;

    int pageOffset = 23;
    int entryIndex = 12;

    byte[] value = new byte[71];

    Random random = new Random();
    random.nextBytes(value);

    OSBTreeBonsaiBucketUpdateValueOperation operation = new OSBTreeBonsaiBucketUpdateValueOperation(lsn, fileId, pageIndex,
        pageOffset, entryIndex, value);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketUpdateValueOperation restored = new OSBTreeBonsaiBucketUpdateValueOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(entryIndex, restored.getEntryIndex());
    Assert.assertArrayEquals(value, restored.getValue());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(34, 55);
    long fileId = 90;
    long pageIndex = 12;

    int pageOffset = 23;
    int entryIndex = 12;

    byte[] value = new byte[71];

    Random random = new Random();
    random.nextBytes(value);

    OSBTreeBonsaiBucketUpdateValueOperation operation = new OSBTreeBonsaiBucketUpdateValueOperation(lsn, fileId, pageIndex,
        pageOffset, entryIndex, value);

    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketUpdateValueOperation restored = new OSBTreeBonsaiBucketUpdateValueOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(entryIndex, restored.getEntryIndex());
    Assert.assertArrayEquals(value, restored.getValue());
  }
}
