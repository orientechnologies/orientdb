package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBonsaiBucketSetRightSiblingOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 45);
    long fileId = 21;
    long pageIndex = 5;

    int pageOffset = 98;
    OBonsaiBucketPointer rightSibling = new OBonsaiBucketPointer(1, 2, 3);

    OSBTreeBonsaiBucketSetRightSiblingOperation operation = new OSBTreeBonsaiBucketSetRightSiblingOperation(lsn, fileId, pageIndex,
        pageOffset, rightSibling);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketSetRightSiblingOperation restored = new OSBTreeBonsaiBucketSetRightSiblingOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(rightSibling, restored.getRightSibling());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 45);
    long fileId = 21;
    long pageIndex = 5;

    int pageOffset = 98;
    OBonsaiBucketPointer rightSibling = new OBonsaiBucketPointer(1, 2, 3);

    OSBTreeBonsaiBucketSetRightSiblingOperation operation = new OSBTreeBonsaiBucketSetRightSiblingOperation(lsn, fileId, pageIndex,
        pageOffset, rightSibling);

    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketSetRightSiblingOperation restored = new OSBTreeBonsaiBucketSetRightSiblingOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(rightSibling, restored.getRightSibling());
  }
}
