package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OMakePositionAvailableOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OMakePositionAvailableOperation makePositionAvailableOperation = new OMakePositionAvailableOperation(unitId, 42, 78);
    final int serializedSize = makePositionAvailableOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = makePositionAvailableOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OMakePositionAvailableOperation restoredMakePositionAvailableOperation = new OMakePositionAvailableOperation();
    offset = restoredMakePositionAvailableOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(makePositionAvailableOperation, restoredMakePositionAvailableOperation);

    Assert.assertEquals(unitId, restoredMakePositionAvailableOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredMakePositionAvailableOperation.getClusterId());
    Assert.assertEquals(78, restoredMakePositionAvailableOperation.getPosition());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OMakePositionAvailableOperation makePositionAvailableOperation = new OMakePositionAvailableOperation(unitId, 42, 78);
    final int serializedSize = makePositionAvailableOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    makePositionAvailableOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OMakePositionAvailableOperation restoredMakePositionAvailableOperation = new OMakePositionAvailableOperation();
    final int offset = restoredMakePositionAvailableOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(makePositionAvailableOperation, restoredMakePositionAvailableOperation);

    Assert.assertEquals(unitId, restoredMakePositionAvailableOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredMakePositionAvailableOperation.getClusterId());
    Assert.assertEquals(78, restoredMakePositionAvailableOperation.getPosition());
  }
}
