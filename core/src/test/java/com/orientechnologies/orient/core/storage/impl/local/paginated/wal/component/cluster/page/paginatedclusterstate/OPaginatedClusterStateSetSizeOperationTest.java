package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.paginatedclusterstate;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OPaginatedClusterStateSetSizeOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);
    long fileId = 34;
    long pageIndex = 23;
    long size = 15;

    OPaginatedClusterStateSetSizeOperation operation = new OPaginatedClusterStateSetSizeOperation(lsn, fileId, pageIndex, size);
    int serializedSize = operation.serializedSize();

    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OPaginatedClusterStateSetSizeOperation restored = new OPaginatedClusterStateSetSizeOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(size, restored.getSize());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);
    long fileId = 34;
    long pageIndex = 23;
    long size = 15;

    OPaginatedClusterStateSetSizeOperation operation = new OPaginatedClusterStateSetSizeOperation(lsn, fileId, pageIndex, size);
    int serializedSize = operation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OPaginatedClusterStateSetSizeOperation restored = new OPaginatedClusterStateSetSizeOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(size, restored.getSize());
  }
}
