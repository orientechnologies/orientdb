package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeBucketUpdateValueOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 9;
    long pageIndex = 98;

    int index = 3;

    byte[] value = new byte[35];
    Random random = new Random();
    random.nextBytes(value);

    OSBTreeBucketUpdateValueOperation operation = new OSBTreeBucketUpdateValueOperation(lsn, fileId, pageIndex, index, value);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OSBTreeBucketUpdateValueOperation restored = new OSBTreeBucketUpdateValueOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(index, restored.getIndex());
    Assert.assertArrayEquals(value, restored.getValue());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 9;
    long pageIndex = 98;

    int index = 3;

    byte[] value = new byte[35];
    Random random = new Random();
    random.nextBytes(value);

    OSBTreeBucketUpdateValueOperation operation = new OSBTreeBucketUpdateValueOperation(lsn, fileId, pageIndex, index, value);
    int serializedSize = operation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketUpdateValueOperation restored = new OSBTreeBucketUpdateValueOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(index, restored.getIndex());
    Assert.assertArrayEquals(value, restored.getValue());
  }
}
