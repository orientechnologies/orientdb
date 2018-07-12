package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OClusterPageDeleteRecordOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 34;
    long pageIndex = 45;
    int recordVersion = 78;
    byte[] record = new byte[34];
    Random random = new Random();
    random.nextBytes(record);

    OClusterPageDeleteRecordOperation clusterPageDeleteRecordOperation = new OClusterPageDeleteRecordOperation(lsn, fileId,
        pageIndex, recordVersion, record);

    final int serializedSize = clusterPageDeleteRecordOperation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = clusterPageDeleteRecordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OClusterPageDeleteRecordOperation restored = new OClusterPageDeleteRecordOperation();
    offset = restored.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordVersion, restored.getRecordVersion());
    Assert.assertArrayEquals(record, restored.getRecord());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 34;
    long pageIndex = 45;
    int recordVersion = 78;
    byte[] record = new byte[34];
    Random random = new Random();
    random.nextBytes(record);

    OClusterPageDeleteRecordOperation clusterPageDeleteRecordOperation = new OClusterPageDeleteRecordOperation(lsn, fileId,
        pageIndex, recordVersion, record);

    final int serializedSize = clusterPageDeleteRecordOperation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    clusterPageDeleteRecordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPageDeleteRecordOperation restored = new OClusterPageDeleteRecordOperation();
    int offset = restored.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordVersion, restored.getRecordVersion());
    Assert.assertArrayEquals(record, restored.getRecord());
  }
}
