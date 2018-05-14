package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OCreateRecordOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final OCreateRecordOperation createRecordOperation = new OCreateRecordOperation(42, unitId, 35, record, 23, (byte) 2);
    final int serializedSize = createRecordOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createRecordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateRecordOperation restoredCreateRecordOperation = new OCreateRecordOperation();
    offset = restoredCreateRecordOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createRecordOperation, restoredCreateRecordOperation);

    Assert.assertEquals(unitId, restoredCreateRecordOperation.getOperationUnitId());
    Assert.assertArrayEquals(record, restoredCreateRecordOperation.getRecord());
    Assert.assertEquals(42, restoredCreateRecordOperation.getClusterId());
    Assert.assertEquals(35, restoredCreateRecordOperation.getPosition());
    Assert.assertEquals(23, restoredCreateRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredCreateRecordOperation.getRecordType());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final OCreateRecordOperation createRecordOperation = new OCreateRecordOperation(42, unitId, 35, record, 23, (byte) 2);
    final int serializedSize = createRecordOperation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createRecordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateRecordOperation restoredCreateRecordOperation = new OCreateRecordOperation();
    final int offset = restoredCreateRecordOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createRecordOperation, restoredCreateRecordOperation);

    Assert.assertEquals(unitId, restoredCreateRecordOperation.getOperationUnitId());
    Assert.assertArrayEquals(record, restoredCreateRecordOperation.getRecord());
    Assert.assertEquals(42, restoredCreateRecordOperation.getClusterId());
    Assert.assertEquals(35, restoredCreateRecordOperation.getPosition());
    Assert.assertEquals(23, restoredCreateRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredCreateRecordOperation.getRecordType());
  }
}
