package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OCreateClusterOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateClusterOperation createClusterOperation = new OCreateClusterOperation(unitId, 42, "caprica", 24);
    final int serializedSize = createClusterOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createClusterOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateClusterOperation restoredCreateClusterOperation = new OCreateClusterOperation();
    offset = restoredCreateClusterOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createClusterOperation, restoredCreateClusterOperation);

    Assert.assertEquals(unitId, restoredCreateClusterOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredCreateClusterOperation.getClusterId());
    Assert.assertEquals("caprica", restoredCreateClusterOperation.getName());
    Assert.assertEquals(24, restoredCreateClusterOperation.getMapFileId());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateClusterOperation createClusterOperation = new OCreateClusterOperation(unitId, 42, "caprica", 24);
    final int serializedSize = createClusterOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createClusterOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateClusterOperation restoredCreateClusterOperation = new OCreateClusterOperation();
    final int offset = restoredCreateClusterOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createClusterOperation, restoredCreateClusterOperation);

    Assert.assertEquals(unitId, restoredCreateClusterOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredCreateClusterOperation.getClusterId());
    Assert.assertEquals("caprica", restoredCreateClusterOperation.getName());
    Assert.assertEquals(24, restoredCreateClusterOperation.getMapFileId());
  }
}
