package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBonsaiBucketSetFreeListPointerOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 98;
    long pageIndex = 34;

    int pageOffset = 23;
    OBonsaiBucketPointer bucketPointer = new OBonsaiBucketPointer(1, 2, 3);

    OSBTreeBonsaiBucketSetFreeListPointerOperation operation = new OSBTreeBonsaiBucketSetFreeListPointerOperation(lsn, fileId,
        pageIndex, pageOffset, bucketPointer);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketSetFreeListPointerOperation restored = new OSBTreeBonsaiBucketSetFreeListPointerOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(bucketPointer, restored.getBucketPointer());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 98;
    long pageIndex = 34;

    int pageOffset = 23;
    OBonsaiBucketPointer bucketPointer = new OBonsaiBucketPointer(1, 2, 3);

    OSBTreeBonsaiBucketSetFreeListPointerOperation operation = new OSBTreeBonsaiBucketSetFreeListPointerOperation(lsn, fileId,
        pageIndex, pageOffset, bucketPointer);
    int serializedSize = operation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketSetFreeListPointerOperation restored = new OSBTreeBonsaiBucketSetFreeListPointerOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(bucketPointer, restored.getBucketPointer());
  }
}
