package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai.page.sbtreebonsaibucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OSBTreeBonsaiBucketSetDeletedOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 67;
    long pageIndex = 89;

    int pageOffset = 90;
    boolean isDeleted = true;

    OSBTreeBonsaiBucketSetDeletedOperation operation = new OSBTreeBonsaiBucketSetDeletedOperation(lsn, fileId, pageIndex,
        pageOffset, isDeleted);
    int serializedSize = operation.serializedSize();

    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OSBTreeBonsaiBucketSetDeletedOperation restored = new OSBTreeBonsaiBucketSetDeletedOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(isDeleted, restored.isDeleted());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 34);
    long fileId = 67;
    long pageIndex = 89;

    int pageOffset = 90;
    boolean isDeleted = true;

    OSBTreeBonsaiBucketSetDeletedOperation operation = new OSBTreeBonsaiBucketSetDeletedOperation(lsn, fileId, pageIndex,
        pageOffset, isDeleted);
    int serializedSize = operation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OSBTreeBonsaiBucketSetDeletedOperation restored = new OSBTreeBonsaiBucketSetDeletedOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(pageOffset, restored.getPageOffset());
    Assert.assertEquals(isDeleted, restored.isDeleted());
  }
}
