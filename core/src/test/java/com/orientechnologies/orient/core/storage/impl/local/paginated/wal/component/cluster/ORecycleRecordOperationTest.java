package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class ORecycleRecordOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final ORecycleRecordOperation recycleRecordOperation = new ORecycleRecordOperation(unitId, 42, 45, record, 12, (byte) 2);
    final int serializedSize = recycleRecordOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = recycleRecordOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final ORecycleRecordOperation restoredRecycleRecordOperation = new ORecycleRecordOperation();
    offset = restoredRecycleRecordOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(recycleRecordOperation, restoredRecycleRecordOperation);

    Assert.assertEquals(unitId, restoredRecycleRecordOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredRecycleRecordOperation.getClusterId());
    Assert.assertEquals(45, restoredRecycleRecordOperation.getClusterPosition());
    Assert.assertArrayEquals(record, restoredRecycleRecordOperation.getRecord());
    Assert.assertEquals(12, restoredRecycleRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredRecycleRecordOperation.getRecordType());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte[] record = new byte[35];
    final Random random = new Random();
    random.nextBytes(record);

    final ORecycleRecordOperation recycleRecordOperation = new ORecycleRecordOperation(unitId, 42, 45, record, 12, (byte) 2);
    final int serializedSize = recycleRecordOperation.serializedSize();
    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    recycleRecordOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final ORecycleRecordOperation restoredRecycleRecordOperation = new ORecycleRecordOperation();
    final int offset = restoredRecycleRecordOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(recycleRecordOperation, restoredRecycleRecordOperation);

    Assert.assertEquals(unitId, restoredRecycleRecordOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredRecycleRecordOperation.getClusterId());
    Assert.assertEquals(45, restoredRecycleRecordOperation.getClusterPosition());
    Assert.assertArrayEquals(record, restoredRecycleRecordOperation.getRecord());
    Assert.assertEquals(12, restoredRecycleRecordOperation.getRecordVersion());
    Assert.assertEquals(2, restoredRecycleRecordOperation.getRecordType());
  }

}
