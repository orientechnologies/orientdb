package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpositionmapbucket;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPositionMapBucketAddOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 45);
    long fileId = 35;
    long pageIndex = 12;
    int recordIndex = 7;

    OClusterPositionMapBucketAddOperation recordOperation = new OClusterPositionMapBucketAddOperation(lsn, fileId, pageIndex,
        recordIndex);
    int serializedSize = recordOperation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = recordOperation.toStream(content, 1);

    Assert.assertEquals(content.length, offset);

    OClusterPositionMapBucketAddOperation restored = new OClusterPositionMapBucketAddOperation();
    restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 45);
    long fileId = 35;
    long pageIndex = 12;
    int recordIndex = 7;

    OClusterPositionMapBucketAddOperation recordOperation = new OClusterPositionMapBucketAddOperation(lsn, fileId, pageIndex,
        recordIndex);
    int serializedSize = recordOperation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    recordOperation.toStream(buffer);

    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPositionMapBucketAddOperation restored = new OClusterPositionMapBucketAddOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());

  }
}
