package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtree;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OCreateSBTreeOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateSBTreeOperation createSBTreeOperation = new OCreateSBTreeOperation(unitId, "caprica", 42);
    final int serializedSize = createSBTreeOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createSBTreeOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateSBTreeOperation restoredSBTreeOperation = new OCreateSBTreeOperation();
    offset = restoredSBTreeOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createSBTreeOperation, restoredSBTreeOperation);
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateSBTreeOperation createSBTreeOperation = new OCreateSBTreeOperation(unitId, "caprica", 42);
    final int serializedSize = createSBTreeOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createSBTreeOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateSBTreeOperation restoredSBTreeOperation = new OCreateSBTreeOperation();
    final int offset = restoredSBTreeOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createSBTreeOperation, restoredSBTreeOperation);
  }
}
