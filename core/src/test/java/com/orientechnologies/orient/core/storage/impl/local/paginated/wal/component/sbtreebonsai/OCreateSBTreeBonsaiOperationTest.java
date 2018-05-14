package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OCreateSBTreeBonsaiOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateSBTreeBonsaiOperation createSBTreeBonsaiOperation = new OCreateSBTreeBonsaiOperation(unitId, 42, "caprica");

    final int serializedSize = createSBTreeBonsaiOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createSBTreeBonsaiOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateSBTreeBonsaiOperation restoredSBTreeBonsaiOperation = new OCreateSBTreeBonsaiOperation();
    offset = restoredSBTreeBonsaiOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createSBTreeBonsaiOperation, restoredSBTreeBonsaiOperation);

    Assert.assertEquals(unitId, restoredSBTreeBonsaiOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredSBTreeBonsaiOperation.getFileId());
    Assert.assertEquals("caprica", restoredSBTreeBonsaiOperation.getName());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OCreateSBTreeBonsaiOperation createSBTreeBonsaiOperation = new OCreateSBTreeBonsaiOperation(unitId, 42, "caprica");
    final int serializedSize = createSBTreeBonsaiOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createSBTreeBonsaiOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateSBTreeBonsaiOperation restoredSBTreeBonsaiOperation = new OCreateSBTreeBonsaiOperation();
    final int offset = restoredSBTreeBonsaiOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createSBTreeBonsaiOperation, restoredSBTreeBonsaiOperation);

    Assert.assertEquals(unitId, restoredSBTreeBonsaiOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredSBTreeBonsaiOperation.getFileId());
    Assert.assertEquals("caprica", restoredSBTreeBonsaiOperation.getName());
  }
}
