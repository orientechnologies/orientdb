package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBucketSetRightSiblingOperationTest {

  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long filedId = 90;
    long pageIndex = 8;

    long rightSibling = 3;

    OSBTreeBucketSetRightSiblingOperation operation = new OSBTreeBucketSetRightSiblingOperation(lsn, filedId, pageIndex,
        rightSibling);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBucketSetRightSiblingOperation restored = new OSBTreeBucketSetRightSiblingOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(filedId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(rightSibling, restored.getRightSibling());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long filedId = 90;
    long pageIndex = 8;

    long rightSibling = 3;

    OSBTreeBucketSetRightSiblingOperation operation = new OSBTreeBucketSetRightSiblingOperation(lsn, filedId, pageIndex,
        rightSibling);

    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketSetRightSiblingOperation restored = new OSBTreeBucketSetRightSiblingOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(filedId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(rightSibling, restored.getRightSibling());
  }
}
