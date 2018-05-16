package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.cluster;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OAllocatePositionOperationTest {
  public static String buildDirectory;

  @BeforeClass
  public static void beforeClass() {
    buildDirectory = System.getProperty("buildDirectory", "./target");
  }

  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OAllocatePositionOperation allocatePositionOperation = new OAllocatePositionOperation(unitId, 42, 78, (byte) 2);
    final int serializedSize = allocatePositionOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = allocatePositionOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OAllocatePositionOperation restoredAllocatePositionOperation = new OAllocatePositionOperation();
    offset = restoredAllocatePositionOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(allocatePositionOperation, restoredAllocatePositionOperation);

    Assert.assertEquals(unitId, restoredAllocatePositionOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredAllocatePositionOperation.getClusterId());
    Assert.assertEquals(78, restoredAllocatePositionOperation.getPosition());
    Assert.assertEquals(2, restoredAllocatePositionOperation.getRecordType());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final OAllocatePositionOperation allocatePositionOperation = new OAllocatePositionOperation(unitId, 42, 78, (byte) 2);
    final int serializedSize = allocatePositionOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    allocatePositionOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OAllocatePositionOperation restoredAllocatePositionOperation = new OAllocatePositionOperation();
    final int offset = restoredAllocatePositionOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(allocatePositionOperation, restoredAllocatePositionOperation);

    Assert.assertEquals(unitId, restoredAllocatePositionOperation.getOperationUnitId());
    Assert.assertEquals(42, restoredAllocatePositionOperation.getClusterId());
    Assert.assertEquals(78, restoredAllocatePositionOperation.getPosition());
    Assert.assertEquals(2, restoredAllocatePositionOperation.getRecordType());
  }
}
