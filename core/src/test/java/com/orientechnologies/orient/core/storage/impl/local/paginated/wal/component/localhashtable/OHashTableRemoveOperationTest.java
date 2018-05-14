package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OHashTableRemoveOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final OHashTableRemoveOperation removeOperation = new OHashTableRemoveOperation(unitId, name, key, value);
    final int serializedSize = removeOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = removeOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OHashTableRemoveOperation restoredRemoveOperation = new OHashTableRemoveOperation();
    offset = restoredRemoveOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(removeOperation, restoredRemoveOperation);
  }

  @Test
  public void testSerializationArrayNullKey() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] value = new byte[27];

    random.nextBytes(value);

    final OHashTableRemoveOperation removeOperation = new OHashTableRemoveOperation(unitId, name, null, value);
    final int serializedSize = removeOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = removeOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OHashTableRemoveOperation restoredRemoveOperation = new OHashTableRemoveOperation();
    offset = restoredRemoveOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(removeOperation, restoredRemoveOperation);
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final OHashTableRemoveOperation removeOperation = new OHashTableRemoveOperation(unitId, name, key, value);
    final int serializedSize = removeOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    removeOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OHashTableRemoveOperation restoredRemoveOperation = new OHashTableRemoveOperation();
    final int offset = restoredRemoveOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(removeOperation, restoredRemoveOperation);
  }

  @Test
  public void testSerializationBufferNullKey() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] value = new byte[27];

    random.nextBytes(value);

    final OHashTableRemoveOperation removeOperation = new OHashTableRemoveOperation(unitId, name, null, value);
    final int serializedSize = removeOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    removeOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OHashTableRemoveOperation restoredRemoveOperation = new OHashTableRemoveOperation();
    final int offset = restoredRemoveOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(removeOperation, restoredRemoveOperation);
  }
}
