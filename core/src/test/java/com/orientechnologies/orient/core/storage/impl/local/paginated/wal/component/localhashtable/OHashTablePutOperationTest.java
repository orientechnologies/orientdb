package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OHashTablePutOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];
    byte[] oldValue = new byte[12];

    random.nextBytes(key);
    random.nextBytes(value);
    random.nextBytes(oldValue);

    final OHashTablePutOperation putOperation = new OHashTablePutOperation(unitId, name, key, value, oldValue);
    final int serializedSize = putOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = putOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OHashTablePutOperation restoredPutOperation = new OHashTablePutOperation();
    offset = restoredPutOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, restoredPutOperation.getOperationUnitId());
    Assert.assertEquals(name, restoredPutOperation.getName());
    Assert.assertArrayEquals(key, restoredPutOperation.getKey());
    Assert.assertArrayEquals(value, restoredPutOperation.getValue());
    Assert.assertArrayEquals(oldValue, restoredPutOperation.getOldValue());
  }

  @Test
  public void testSerializationArrayKeyNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] value = new byte[27];
    byte[] oldValue = new byte[12];

    random.nextBytes(value);
    random.nextBytes(oldValue);

    final OHashTablePutOperation putOperation = new OHashTablePutOperation(unitId, name, null, value, oldValue);
    final int serializedSize = putOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = putOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OHashTablePutOperation restoredPutOperation = new OHashTablePutOperation();
    offset = restoredPutOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, restoredPutOperation.getOperationUnitId());
    Assert.assertEquals(name, restoredPutOperation.getName());
    Assert.assertArrayEquals(null, restoredPutOperation.getKey());
    Assert.assertArrayEquals(value, restoredPutOperation.getValue());
    Assert.assertArrayEquals(oldValue, restoredPutOperation.getOldValue());
  }

  @Test
  public void testSerializationArrayOldValueNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final OHashTablePutOperation putOperation = new OHashTablePutOperation(unitId, name, key, value, null);
    final int serializedSize = putOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = putOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OHashTablePutOperation restoredPutOperation = new OHashTablePutOperation();
    offset = restoredPutOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, restoredPutOperation.getOperationUnitId());
    Assert.assertEquals(name, restoredPutOperation.getName());
    Assert.assertArrayEquals(key, restoredPutOperation.getKey());
    Assert.assertArrayEquals(value, restoredPutOperation.getValue());
    Assert.assertArrayEquals(null, restoredPutOperation.getOldValue());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];
    byte[] oldValue = new byte[12];

    random.nextBytes(key);
    random.nextBytes(value);
    random.nextBytes(oldValue);

    final OHashTablePutOperation putOperation = new OHashTablePutOperation(unitId, name, key, value, oldValue);
    final int serializedSize = putOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    putOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OHashTablePutOperation restoredPutOperation = new OHashTablePutOperation();
    final int offset = restoredPutOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, restoredPutOperation.getOperationUnitId());
    Assert.assertEquals(name, restoredPutOperation.getName());
    Assert.assertArrayEquals(key, restoredPutOperation.getKey());
    Assert.assertArrayEquals(value, restoredPutOperation.getValue());
    Assert.assertArrayEquals(oldValue, restoredPutOperation.getOldValue());
  }

  @Test
  public void testSerializationBufferOldValueNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final OHashTablePutOperation putOperation = new OHashTablePutOperation(unitId, name, key, value, null);
    final int serializedSize = putOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    putOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OHashTablePutOperation restoredPutOperation = new OHashTablePutOperation();
    final int offset = restoredPutOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, restoredPutOperation.getOperationUnitId());
    Assert.assertEquals(name, restoredPutOperation.getName());
    Assert.assertArrayEquals(key, restoredPutOperation.getKey());
    Assert.assertArrayEquals(value, restoredPutOperation.getValue());
    Assert.assertArrayEquals(null, restoredPutOperation.getOldValue());
  }

  @Test
  public void testSerializationBufferKeyNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    String name = "caprica";
    Random random = new Random();

    byte[] value = new byte[27];
    byte[] oldValue = new byte[12];

    random.nextBytes(value);
    random.nextBytes(oldValue);

    final OHashTablePutOperation putOperation = new OHashTablePutOperation(unitId, name, null, value, oldValue);
    final int serializedSize = putOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    putOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OHashTablePutOperation restoredPutOperation = new OHashTablePutOperation();
    final int offset = restoredPutOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, restoredPutOperation.getOperationUnitId());
    Assert.assertEquals(name, restoredPutOperation.getName());
    Assert.assertArrayEquals(null, restoredPutOperation.getKey());
    Assert.assertArrayEquals(value, restoredPutOperation.getValue());
    Assert.assertArrayEquals(oldValue, restoredPutOperation.getOldValue());
  }
}
