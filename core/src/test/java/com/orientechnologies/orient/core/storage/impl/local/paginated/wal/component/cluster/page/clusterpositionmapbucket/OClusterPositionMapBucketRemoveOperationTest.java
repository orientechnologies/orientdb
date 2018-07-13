package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapBucketRemoveOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(13, 56);
    long fileId = 56;
    long pageIndex = 90;
    int recordIndex = 5;
    int recordPosition = 9;
    long recordPageIndex = 34;

    OClusterPositionMapBucketRemoveOperation operation = new OClusterPositionMapBucketRemoveOperation(lsn, fileId, pageIndex,
        recordIndex, recordPosition, recordPageIndex);
    int serializedSize = operation.serializedSize();

    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OClusterPositionMapBucketRemoveOperation restored = new OClusterPositionMapBucketRemoveOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(recordIndex, restored.getRecordIndex());
    Assert.assertEquals(recordPosition, restored.getRecordPosition());
    Assert.assertEquals(recordPageIndex, restored.getRecordPageIndex());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(13, 56);
    long fileId = 56;
    long pageIndex = 90;
    int recordIndex = 5;
    int recordPosition = 9;
    long recordPageIndex = 34;

    OClusterPositionMapBucketRemoveOperation operation = new OClusterPositionMapBucketRemoveOperation(lsn, fileId, pageIndex,
        recordIndex, recordPosition, recordPageIndex);
    int serializedSize = operation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapBucketRemoveOperation restored = new OClusterPositionMapBucketRemoveOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(recordIndex, restored.getRecordIndex());
    Assert.assertEquals(recordPosition, restored.getRecordPosition());
    Assert.assertEquals(recordPageIndex, restored.getRecordPageIndex());
  }
}
