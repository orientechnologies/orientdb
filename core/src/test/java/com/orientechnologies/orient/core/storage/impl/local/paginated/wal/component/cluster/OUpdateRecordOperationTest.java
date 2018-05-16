package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OUpdateRecordOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final byte[] prevRecord = new byte[47];
    random.nextBytes(prevRecord);

    final OUpdateRecordOperation updateRecordOperation = new OUpdateRecordOperation(unitId, 42, 128, record, 73, (byte) 2,
        prevRecord, 85, (byte) 3);
    final int serializedSize = updateRecordOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = updateRecordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OUpdateRecordOperation restoredUpdateRecordOperation = new OUpdateRecordOperation();
    offset = restoredUpdateRecordOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(updateRecordOperation, restoredUpdateRecordOperation);

    Assert.assertEquals(unitId, restoredUpdateRecordOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredUpdateRecordOperation.getClusterId());
    Assert.assertEquals(128, restoredUpdateRecordOperation.getClusterPosition());
    Assert.assertArrayEquals(record, restoredUpdateRecordOperation.getRecord());
    Assert.assertEquals(73, restoredUpdateRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredUpdateRecordOperation.getRecordType());
    Assert.assertArrayEquals(prevRecord, restoredUpdateRecordOperation.getPrevRecord());
    Assert.assertEquals(85, restoredUpdateRecordOperation.getPrevRecordVersion());
    Assert.assertEquals(3, restoredUpdateRecordOperation.getPrevRecordType());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final byte[] prevRecord = new byte[47];
    random.nextBytes(prevRecord);

    final OUpdateRecordOperation updateRecordOperation = new OUpdateRecordOperation(unitId, 42, 128, record, 73, (byte) 2,
        prevRecord, 85, (byte) 3);
    final int serializedSize = updateRecordOperation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    updateRecordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OUpdateRecordOperation restoredUpdateRecordOperation = new OUpdateRecordOperation();
    final int offset = restoredUpdateRecordOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(updateRecordOperation, restoredUpdateRecordOperation);

    Assert.assertEquals(unitId, restoredUpdateRecordOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredUpdateRecordOperation.getClusterId());
    Assert.assertEquals(128, restoredUpdateRecordOperation.getClusterPosition());
    Assert.assertArrayEquals(record, restoredUpdateRecordOperation.getRecord());
    Assert.assertEquals(73, restoredUpdateRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredUpdateRecordOperation.getRecordType());
    Assert.assertArrayEquals(prevRecord, restoredUpdateRecordOperation.getPrevRecord());
    Assert.assertEquals(85, restoredUpdateRecordOperation.getPrevRecordVersion());
    Assert.assertEquals(3, restoredUpdateRecordOperation.getPrevRecordType());
  }
}
