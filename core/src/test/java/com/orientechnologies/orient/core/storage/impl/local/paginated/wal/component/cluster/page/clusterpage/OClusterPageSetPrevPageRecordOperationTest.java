package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster.page.clusterpage;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OClusterPageSetPrevPageRecordOperationTest {
  @Test
  public void testArraySerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 23;
    long pageIndex = 57;
    long prevPage = 12;

    OClusterPageSetPrevPageRecordOperation recordOperation = new OClusterPageSetPrevPageRecordOperation(lsn, fileId, pageIndex,
        prevPage);
    int serializedSize = recordOperation.serializedSize();
    byte[] content = new byte[serializedSize + 1];
    int offset = recordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    OClusterPageSetPrevPageRecordOperation restored = new OClusterPageSetPrevPageRecordOperation();
    offset = restored.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(prevPage, restored.getPrevPage());
  }

  @Test
  public void testBufferSerialization() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(12, 56);
    long fileId = 23;
    long pageIndex = 57;
    long prevPage = 12;

    OClusterPageSetPrevPageRecordOperation recordOperation = new OClusterPageSetPrevPageRecordOperation(lsn, fileId, pageIndex,
        prevPage);
    int serializedSize = recordOperation.serializedSize();
    ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    recordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    OClusterPageSetPrevPageRecordOperation restored = new OClusterPageSetPrevPageRecordOperation();
    int offset = restored.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);

    Assert.assertEquals(lsn, restored.getPageLSN());
    Assert.assertEquals(fileId, restored.getFileId());
    Assert.assertEquals(pageIndex, restored.getPageIndex());
    Assert.assertEquals(prevPage, restored.getPrevPage());
  }
}
