package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.paginatedclusterstate;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

public class OPaginatedClusterStateSetRecordSizeOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);

    long fileId = 6;
    long pageIndex = 9;

    long recordSize = 12;

    OPaginatedClusterStateSetRecordSizeOperation operation = new OPaginatedClusterStateSetRecordSizeOperation(lsn, fileId,
        pageIndex, recordSize);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OPaginatedClusterStateSetRecordSizeOperation restored = new OPaginatedClusterStateSetRecordSizeOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(recordSize, restored.getRecordSize());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);

    long fileId = 6;
    long pageIndex = 9;

    long recordSize = 12;

    OPaginatedClusterStateSetRecordSizeOperation operation = new OPaginatedClusterStateSetRecordSizeOperation(lsn, fileId,
        pageIndex, recordSize);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OPaginatedClusterStateSetRecordSizeOperation restored = new OPaginatedClusterStateSetRecordSizeOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(recordSize, restored.getRecordSize());
  }
}
