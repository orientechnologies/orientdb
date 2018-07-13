package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapBucketResurrectOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(34, 56);
    long fileId = 24;
    long pageIndex = 23;
    int recordIndex = 7;

    OClusterPositionMapBucketResurrectOperation operation = new OClusterPositionMapBucketResurrectOperation(lsn, fileId, pageIndex,
        recordIndex);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OClusterPositionMapBucketResurrectOperation restored = new OClusterPositionMapBucketResurrectOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(recordIndex, restored.getRecordIndex());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(34, 56);
    long fileId = 24;
    long pageIndex = 23;
    int recordIndex = 7;

    OClusterPositionMapBucketResurrectOperation operation = new OClusterPositionMapBucketResurrectOperation(lsn, fileId, pageIndex,
        recordIndex);
    int serializedSize = operation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    operation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapBucketResurrectOperation restored = new OClusterPositionMapBucketResurrectOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(recordIndex, restored.getRecordIndex());
  }
}
