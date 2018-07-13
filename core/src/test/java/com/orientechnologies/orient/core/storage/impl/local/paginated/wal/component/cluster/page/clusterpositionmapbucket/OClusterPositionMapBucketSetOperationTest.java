package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapBucketSetOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 89);
    long fileId = 9;
    long pageIndex = 78;
    int recordIndex = 3;
    byte flag = 8;
    long recordPageIndex = 45;
    int recordPosition = 12;

    OClusterPositionMapBucketSetOperation operation = new OClusterPositionMapBucketSetOperation(lsn, fileId, pageIndex, recordIndex,
        flag, recordPageIndex, recordPosition);
    int serializedSize = operation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = operation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OClusterPositionMapBucketSetOperation restored = new OClusterPositionMapBucketSetOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(flag, restored.getFlag());
    Assert.assertEquals(recordPageIndex, restored.getRecordPageIndex());
    Assert.assertEquals(recordPosition, restored.getRecordPosition());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 89);
    long fileId = 9;
    long pageIndex = 78;
    int recordIndex = 3;
    byte flag = 8;
    long recordPageIndex = 45;
    int recordPosition = 12;

    OClusterPositionMapBucketSetOperation operation = new OClusterPositionMapBucketSetOperation(lsn, fileId, pageIndex, recordIndex,
        flag, recordPageIndex, recordPosition);
    int serializedSize = operation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    operation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapBucketSetOperation restored = new OClusterPositionMapBucketSetOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());

    Assert.assertEquals(flag, restored.getFlag());
    Assert.assertEquals(recordPageIndex, restored.getRecordPageIndex());
    Assert.assertEquals(recordPosition, restored.getRecordPosition());
  }
}
