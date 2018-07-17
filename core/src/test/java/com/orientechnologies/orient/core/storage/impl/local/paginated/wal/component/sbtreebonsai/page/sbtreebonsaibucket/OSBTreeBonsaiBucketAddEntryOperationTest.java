package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBonsaiBucketAddEntryOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(3, 45);
    long fileId = 5;
    long pageIndex = 23;

    int pageOffset = 56;
    int entryIndex = 32;

    OSBTreeBonsaiBucketAddEntryOperation operation = new OSBTreeBonsaiBucketAddEntryOperation(lsn, fileId, pageIndex, pageOffset,
        entryIndex);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketAddEntryOperation restored = new OSBTreeBonsaiBucketAddEntryOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(entryIndex, restored.getEntryIndex());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(3, 45);
    long fileId = 5;
    long pageIndex = 23;

    int pageOffset = 56;
    int entryIndex = 32;

    OSBTreeBonsaiBucketAddEntryOperation operation = new OSBTreeBonsaiBucketAddEntryOperation(lsn, fileId, pageIndex, pageOffset,
        entryIndex);
    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketAddEntryOperation restored = new OSBTreeBonsaiBucketAddEntryOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(entryIndex, restored.getEntryIndex());
  }
}
