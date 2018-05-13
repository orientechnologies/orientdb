package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.sbtreebonsai;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

public class ORemoveOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    long fileId = 124;
    OBonsaiBucketPointer pointer = new OBonsaiBucketPointer(25, 31);

    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final ORemoveOperation removeOperation = new ORemoveOperation(unitId, fileId, pointer, key, value);
    final int serializedSize = removeOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = removeOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final ORemoveOperation restoredRemoveOperation = new ORemoveOperation();
    offset = restoredRemoveOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(removeOperation, restoredRemoveOperation);
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    long fileId = 124;
    OBonsaiBucketPointer pointer = new OBonsaiBucketPointer(25, 31);

    Random random = new Random();

    byte[] key = new byte[35];
    byte[] value = new byte[27];

    random.nextBytes(key);
    random.nextBytes(value);

    final ORemoveOperation removeOperation = new ORemoveOperation(unitId, fileId, pointer, key, value);
    final int serializedSize = removeOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    removeOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final ORemoveOperation restoredRemoveOperation = new ORemoveOperation();
    final int offset = restoredRemoveOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(removeOperation, restoredRemoveOperation);
  }
}
