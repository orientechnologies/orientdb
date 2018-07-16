package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree.page.sbtreebucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBucketSetSizeOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 56;
    long pageIndex = 12;

    long treeSize = 123;

    OSBTreeBucketSetSizeOperation operation = new OSBTreeBucketSetSizeOperation(lsn, fileId, pageIndex, treeSize);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OSBTreeBucketSetSizeOperation restored = new OSBTreeBucketSetSizeOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(treeSize, restored.getTreeSize());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 56;
    long pageIndex = 12;

    long treeSize = 123;

    OSBTreeBucketSetSizeOperation operation = new OSBTreeBucketSetSizeOperation(lsn, fileId, pageIndex, treeSize);
    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBucketSetSizeOperation restored = new OSBTreeBucketSetSizeOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(treeSize, restored.getTreeSize());
  }
}
