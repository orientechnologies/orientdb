package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class OSBTreeBonsaiPutOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    long fileId = 124;
    OBonsaiBucketPointer pointer = new OBonsaiBucketPointer(456, 23);
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];
    byte[] oldValue = new byte[12];

    random.nextBytes(key);
    random.nextBytes(value);
    random.nextBytes(oldValue);

    final OSBTreeBonsaiPutOperation putOperation = new OSBTreeBonsaiPutOperation(unitId, fileId, pointer, key, value, oldValue);
    final int serializedSize = putOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];

    int offset = putOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OSBTreeBonsaiPutOperation restoredPutOperation = new OSBTreeBonsaiPutOperation();
    offset = restoredPutOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, putOperation.getOperationUnitId());
    Assert.assertEquals(fileId, putOperation.getFileId());
    Assert.assertEquals(pointer, putOperation.getPointer());
    Assert.assertArrayEquals(key, putOperation.getKey());
    Assert.assertArrayEquals(value, putOperation.getValue());
    Assert.assertArrayEquals(oldValue, putOperation.getOldValue());
  }

  @Test
  public void testSerializationArrayOldValueNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    long fileId = 124;
    OBonsaiBucketPointer pointer = new OBonsaiBucketPointer(456, 23);
    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final OSBTreeBonsaiPutOperation putOperation = new OSBTreeBonsaiPutOperation(unitId, fileId, pointer, key, value, null);
    final int serializedSize = putOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = putOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OSBTreeBonsaiPutOperation restoredPutOperation = new OSBTreeBonsaiPutOperation();
    offset = restoredPutOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, putOperation.getOperationUnitId());
    Assert.assertEquals(fileId, putOperation.getFileId());
    Assert.assertEquals(pointer, putOperation.getPointer());
    Assert.assertArrayEquals(key, putOperation.getKey());
    Assert.assertArrayEquals(value, putOperation.getValue());
    Assert.assertArrayEquals(null, putOperation.getOldValue());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    long fileId = 124;
    OBonsaiBucketPointer pointer = new OBonsaiBucketPointer(456, 23);

    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];
    byte[] oldValue = new byte[12];

    random.nextBytes(key);
    random.nextBytes(value);
    random.nextBytes(oldValue);

    final OSBTreeBonsaiPutOperation putOperation = new OSBTreeBonsaiPutOperation(unitId, fileId, pointer, key, value, oldValue);
    final int serializedSize = putOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    putOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OSBTreeBonsaiPutOperation restoredPutOperation = new OSBTreeBonsaiPutOperation();
    final int offset = restoredPutOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, putOperation.getOperationUnitId());
    Assert.assertEquals(fileId, putOperation.getFileId());
    Assert.assertEquals(pointer, putOperation.getPointer());
    Assert.assertArrayEquals(key, putOperation.getKey());
    Assert.assertArrayEquals(value, putOperation.getValue());
    Assert.assertArrayEquals(oldValue, putOperation.getOldValue());
  }

  @Test
  public void testSerializationBufferOldValueNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    long fileId = 124;
    OBonsaiBucketPointer pointer = new OBonsaiBucketPointer(456, 23);

    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final OSBTreeBonsaiPutOperation putOperation = new OSBTreeBonsaiPutOperation(unitId, fileId, pointer, key, value, null);
    final int serializedSize = putOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    putOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OSBTreeBonsaiPutOperation restoredPutOperation = new OSBTreeBonsaiPutOperation();
    final int offset = restoredPutOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(putOperation, restoredPutOperation);

    Assert.assertEquals(unitId, putOperation.getOperationUnitId());
    Assert.assertEquals(fileId, putOperation.getFileId());
    Assert.assertEquals(pointer, putOperation.getPointer());
    Assert.assertArrayEquals(key, putOperation.getKey());
    Assert.assertArrayEquals(value, putOperation.getValue());
    Assert.assertArrayEquals(null, putOperation.getOldValue());
  }
}
