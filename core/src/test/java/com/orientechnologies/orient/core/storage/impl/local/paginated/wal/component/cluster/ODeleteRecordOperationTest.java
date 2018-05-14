package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class ODeleteRecordOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final ODeleteRecordOperation deleteRecordOperation = new ODeleteRecordOperation(unitId, 42, 45, record, 12, (byte) 2);
    final int serializedSize = deleteRecordOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = deleteRecordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final ODeleteRecordOperation restoredDeleteRecordOperation = new ODeleteRecordOperation();
    offset = restoredDeleteRecordOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(deleteRecordOperation, restoredDeleteRecordOperation);

    Assert.assertEquals(unitId, restoredDeleteRecordOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredDeleteRecordOperation.getClusterId());
    Assert.assertEquals(45, restoredDeleteRecordOperation.getClusterPosition());
    Assert.assertArrayEquals(record, restoredDeleteRecordOperation.getRecord());
    Assert.assertEquals(12, restoredDeleteRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredDeleteRecordOperation.getRecordType());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final ODeleteRecordOperation deleteRecordOperation = new ODeleteRecordOperation(unitId, 42, 45, record, 12, (byte) 2);
    final int serializedSize = deleteRecordOperation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    deleteRecordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final ODeleteRecordOperation restoredDeleteRecordOperation = new ODeleteRecordOperation();
    final int offset = restoredDeleteRecordOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(deleteRecordOperation, restoredDeleteRecordOperation);

    Assert.assertEquals(unitId, restoredDeleteRecordOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredDeleteRecordOperation.getClusterId());
    Assert.assertEquals(45, restoredDeleteRecordOperation.getClusterPosition());
    Assert.assertArrayEquals(record, restoredDeleteRecordOperation.getRecord());
    Assert.assertEquals(12, restoredDeleteRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredDeleteRecordOperation.getRecordType());
  }
}
