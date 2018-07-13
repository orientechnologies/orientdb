package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapBucketAllocateOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 89);
    long fileId = 34;
    long pageIndex = 35;
    int recordIndex = 7;

    OClusterPositionMapBucketAllocateOperation recordOperation = new OClusterPositionMapBucketAllocateOperation(lsn, fileId,
        pageIndex, recordIndex);

    int serializedSize = recordOperation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = recordOperation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OClusterPositionMapBucketAllocateOperation restored = new OClusterPositionMapBucketAllocateOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 89);
    long fileId = 34;
    long pageIndex = 35;
    int recordIndex = 7;

    OClusterPositionMapBucketAllocateOperation recordOperation = new OClusterPositionMapBucketAllocateOperation(lsn, fileId,
        pageIndex, recordIndex);

    int serializedSize = recordOperation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    recordOperation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapBucketAllocateOperation restored = new OClusterPositionMapBucketAllocateOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());
  }
}
