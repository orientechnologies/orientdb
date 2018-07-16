package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBucketAddEntryOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 54);

    long fileId = 25;
    long pageIndex = 67;

    int entryIndex = 2;

    OSBTreeBucketAddEntryOperation operation = new OSBTreeBucketAddEntryOperation(lsn, fileId, pageIndex, entryIndex);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OSBTreeBucketAddEntryOperation restored = new OSBTreeBucketAddEntryOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(entryIndex, restored.getEntryIndex());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 54);

    long fileId = 25;
    long pageIndex = 67;

    int entryIndex = 2;

    OSBTreeBucketAddEntryOperation operation = new OSBTreeBucketAddEntryOperation(lsn, fileId, pageIndex, entryIndex);
    int serializedSize = operation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketAddEntryOperation restored = new OSBTreeBucketAddEntryOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(entryIndex, restored.getEntryIndex());
  }
}
