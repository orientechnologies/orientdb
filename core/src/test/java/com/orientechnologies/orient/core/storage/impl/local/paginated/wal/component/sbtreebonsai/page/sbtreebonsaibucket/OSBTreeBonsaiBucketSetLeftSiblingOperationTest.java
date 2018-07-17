package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import org.junit.Assert;
import org.junit.Test;

public class OSBTreeBonsaiBucketSetLeftSiblingOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 45;
    long pageIndex = 54;

    int pageOffset = 23;
    OBonsaiBucketPointer leftSibling = new OBonsaiBucketPointer(1, 2, 3);

    OSBTreeBonsaiBucketSetLeftSiblingOperation operation = new OSBTreeBonsaiBucketSetLeftSiblingOperation(lsn, fileId, pageIndex,
        pageOffset, leftSibling);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketSetLeftSiblingOperation restored = new OSBTreeBonsaiBucketSetLeftSiblingOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(leftSibling, restored.getLeftSibling());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 45;
    long pageIndex = 54;

    int pageOffset = 23;
    OBonsaiBucketPointer leftSibling = new OBonsaiBucketPointer(1, 2, 3);

    OSBTreeBonsaiBucketSetLeftSiblingOperation operation = new OSBTreeBonsaiBucketSetLeftSiblingOperation(lsn, fileId, pageIndex,
        pageOffset, leftSibling);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketSetLeftSiblingOperation restored = new OSBTreeBonsaiBucketSetLeftSiblingOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(leftSibling, restored.getLeftSibling());
  }
}
