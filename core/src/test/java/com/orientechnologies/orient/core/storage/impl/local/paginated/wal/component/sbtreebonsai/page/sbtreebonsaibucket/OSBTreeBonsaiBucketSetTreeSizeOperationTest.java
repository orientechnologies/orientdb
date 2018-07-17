package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBonsaiBucketSetTreeSizeOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 34;
    long pageIndex = 65;

    int pageOffset = 90;
    long treeSize = 89;

    OSBTreeBonsaiBucketSetTreeSizeOperation operation = new OSBTreeBonsaiBucketSetTreeSizeOperation(lsn, fileId, pageIndex,
        pageOffset, treeSize);

    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketSetTreeSizeOperation restored = new OSBTreeBonsaiBucketSetTreeSizeOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(treeSize, restored.getTreeSize());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 34;
    long pageIndex = 65;

    int pageOffset = 90;
    long treeSize = 89;

    OSBTreeBonsaiBucketSetTreeSizeOperation operation = new OSBTreeBonsaiBucketSetTreeSizeOperation(lsn, fileId, pageIndex,
        pageOffset, treeSize);

    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketSetTreeSizeOperation restored = new OSBTreeBonsaiBucketSetTreeSizeOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(treeSize, restored.getTreeSize());
  }
}
