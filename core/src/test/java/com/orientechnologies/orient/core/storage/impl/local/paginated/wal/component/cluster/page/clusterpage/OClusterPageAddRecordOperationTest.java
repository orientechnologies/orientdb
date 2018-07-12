package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPageAddRecordOperationTest {
  @Test
  public void testSerializationArray() {
    final OLogSequenceNumber lsn = new OLogSequenceNumber(24, 35);
    final long fileId = 15;
    final long pageIndex = 12;
    final int recordIndex = 35;

    OClusterPageAddRecordOperation clusterPageAddRecordOperation = new OClusterPageAddRecordOperation(lsn, fileId, pageIndex,
        recordIndex);
    final int serializedSize = clusterPageAddRecordOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];

    int offset = clusterPageAddRecordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OClusterPageAddRecordOperation restored = new OClusterPageAddRecordOperation();
    offset = restored.fromStream(content, 1);

    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());
  }

  @Test
  public void testBufferSerialization() {
    final OLogSequenceNumber lsn = new OLogSequenceNumber(24, 35);
    final long fileId = 15;
    final long pageIndex = 12;
    final int recordIndex = 35;

    OClusterPageAddRecordOperation clusterPageAddRecordOperation = new OClusterPageAddRecordOperation(lsn, fileId, pageIndex,
        recordIndex);
    final int serializedSize = clusterPageAddRecordOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);

    clusterPageAddRecordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPageAddRecordOperation restored = new OClusterPageAddRecordOperation();
    int offset = restored.fromStream(buffer.array(), 1);

    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(recordIndex, restored.getRecordIndex());
  }
}
