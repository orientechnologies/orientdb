package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OClusterPageReplaceRecordOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);
    long fileId = 34;
    long pageIndex = 27;
    int oldRecordVersion = 23;
    byte[] oldRecord = new byte[45];
    Random random = new Random();
    random.nextBytes(oldRecord);

    OClusterPageReplaceRecordOperation recordOperation = new OClusterPageReplaceRecordOperation(lsn, fileId, pageIndex,
        oldRecordVersion, oldRecord);
    int serializedSize = recordOperation.serializedSize();
    byte[] content = new byte[serializedSize + 1];

    int offset = recordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OClusterPageReplaceRecordOperation restored = new OClusterPageReplaceRecordOperation();
    offset = restored.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(oldRecordVersion, restored.getOldRecordVersion());
    Assert.assertArrayEquals(oldRecord, restored.getOldRecord());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(23, 56);
    long fileId = 34;
    long pageIndex = 27;
    int oldRecordVersion = 23;
    byte[] oldRecord = new byte[45];
    Random random = new Random();
    random.nextBytes(oldRecord);

    OClusterPageReplaceRecordOperation recordOperation = new OClusterPageReplaceRecordOperation(lsn, fileId, pageIndex,
        oldRecordVersion, oldRecord);
    int serializedSize = recordOperation.serializedSize();

    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    recordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPageReplaceRecordOperation restored = new OClusterPageReplaceRecordOperation();
    int offset = restored.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(oldRecordVersion, restored.getOldRecordVersion());
    Assert.assertArrayEquals(oldRecord, restored.getOldRecord());
  }
}
