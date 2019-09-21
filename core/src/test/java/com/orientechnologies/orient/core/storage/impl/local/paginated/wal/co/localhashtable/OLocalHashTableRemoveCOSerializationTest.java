package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.localhashtable;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OLocalHashTableRemoveCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final int indexId = 23;
    final String encryptionName = "encryption";

    final byte keySerializerId = 12;

    final Random random = new Random();

    final byte[] key = new byte[12];
    random.nextBytes(key);

    final byte[] value = new byte[23];
    random.nextBytes(value);

    final byte valueSerializerId = 4;

    OLocalHashTableRemoveCO co = new OLocalHashTableRemoveCO(indexId, encryptionName, keySerializerId, key, value,
        valueSerializerId);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = co.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OLocalHashTableRemoveCO restoredCO = new OLocalHashTableRemoveCO();
    pos = restoredCO.fromStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(valueSerializerId, restoredCO.getValueSerializerId());
    Assert.assertArrayEquals(value, restoredCO.getValue());
  }
}
