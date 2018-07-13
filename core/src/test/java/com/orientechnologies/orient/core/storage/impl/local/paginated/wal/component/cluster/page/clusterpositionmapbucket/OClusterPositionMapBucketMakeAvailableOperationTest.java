package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapBucketMakeAvailableOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(35, 78);
    long fileId = 56;
    long pageIndex = 9;
    int recordIndex = 98;
    int size = 17;

    OClusterPositionMapBucketMakeAvailableOperation recordOperation = new OClusterPositionMapBucketMakeAvailableOperation(lsn,
        fileId, pageIndex, recordIndex, size);
    int serializedSize = recordOperation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = recordOperation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OClusterPositionMapBucketMakeAvailableOperation restored = new OClusterPositionMapBucketMakeAvailableOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());
    Assert.assertEquals(size, restored.getSize());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(35, 78);
    long fileId = 56;
    long pageIndex = 9;
    int recordIndex = 98;
    int size = 17;

    OClusterPositionMapBucketMakeAvailableOperation recordOperation = new OClusterPositionMapBucketMakeAvailableOperation(lsn,
        fileId, pageIndex, recordIndex, size);
    int serializedSize = recordOperation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    recordOperation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapBucketMakeAvailableOperation restored = new OClusterPositionMapBucketMakeAvailableOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());
    Assert.assertEquals(size, restored.getSize());
  }
}
