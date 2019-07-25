package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.localhashtable;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OLocalHashTablePutCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();

    final byte[] value = new byte[12];
    random.nextBytes(value);

    final byte valueSerializerId = 5;

    final int indexId = 12;

    final byte keySerializerId = 10;

    final byte[] prevValue = new byte[5];
    random.nextBytes(prevValue);

    OLocalHashTablePutCO co = new OLocalHashTablePutCO(indexId, null, keySerializerId, null, valueSerializerId, value, prevValue);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = co.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OLocalHashTablePutCO restoredCO = new OLocalHashTablePutCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertArrayEquals(value, restoredCO.getValue());
    Assert.assertArrayEquals(prevValue, restoredCO.getPrevValue());
    Assert.assertEquals(valueSerializerId, restoredCO.getValueSerializerId());
  }

  @Test
  public void testSerializationNull() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();

    final byte[] value = new byte[12];
    random.nextBytes(value);

    final byte valueSerializerId = 5;

    final int indexId = 12;

    final byte keySerializerId = 10;

    final byte[] prevValue = null;

    OLocalHashTablePutCO co = new OLocalHashTablePutCO(indexId, null, keySerializerId, null, valueSerializerId, value, prevValue);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = co.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OLocalHashTablePutCO restoredCO = new OLocalHashTablePutCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertArrayEquals(value, restoredCO.getValue());
    Assert.assertArrayEquals(prevValue, restoredCO.getPrevValue());
    Assert.assertEquals(valueSerializerId, restoredCO.getValueSerializerId());
  }
}
