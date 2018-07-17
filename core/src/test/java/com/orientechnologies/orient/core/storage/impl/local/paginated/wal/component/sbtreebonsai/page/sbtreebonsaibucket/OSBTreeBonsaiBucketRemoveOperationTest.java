package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeBonsaiBucketRemoveOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 56;
    long pageIndex = 78;

    int pageOffset = 90;
    byte[] key = new byte[12];
    byte[] value = new byte[34];
    int entryIndex = 5;

    Random random = new Random();
    random.nextBytes(key);
    random.nextBytes(value);

    OSBTreeBonsaiBucketRemoveOperation operation = new OSBTreeBonsaiBucketRemoveOperation(lsn, fileId, pageIndex, pageOffset, key,
        value, entryIndex);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketRemoveOperation restored = new OSBTreeBonsaiBucketRemoveOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertArrayEquals(key, restored.getKey());
    Assert.assertArrayEquals(value, restored.getValue());
    Assert.assertEquals(entryIndex, restored.getEntryIndex());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 56;
    long pageIndex = 78;

    int pageOffset = 90;
    byte[] key = new byte[12];
    byte[] value = new byte[34];
    int entryIndex = 5;

    Random random = new Random();
    random.nextBytes(key);
    random.nextBytes(value);

    OSBTreeBonsaiBucketRemoveOperation operation = new OSBTreeBonsaiBucketRemoveOperation(lsn, fileId, pageIndex, pageOffset, key,
        value, entryIndex);

    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketRemoveOperation restored = new OSBTreeBonsaiBucketRemoveOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertArrayEquals(key, restored.getKey());
    Assert.assertArrayEquals(value, restored.getValue());
    Assert.assertEquals(entryIndex, restored.getEntryIndex());
  }
}
