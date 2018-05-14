package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OCreateHashTableOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica", 42, 24);
    final int serializedSize = createHashTableOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createHashTableOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    offset = restoredCreateHashTableOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica", 42, 24);
    final int serializedSize = createHashTableOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createHashTableOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    final int offset = restoredCreateHashTableOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);
  }
}
