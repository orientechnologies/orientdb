package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreemultivalue;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OCellBTreeMultiValuePutCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final String encryptionName = "encryption";
    final byte keySerializerId = 12;
    final int indexId = 456;

    final Random random = new Random();
    final byte[] key = new byte[12];
    random.nextBytes(key);

    final ORID value = new ORecordId(12, 38);

    OCellBTreeMultiValuePutCO co = new OCellBTreeMultiValuePutCO(encryptionName, keySerializerId, indexId, key, value);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int pos = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OCellBTreeMultiValuePutCO restoredCO = new OCellBTreeMultiValuePutCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertEquals(value, restoredCO.getValue());
  }

  @Test
  public void testNullSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final String encryptionName = null;
    final byte keySerializerId = 12;
    final int indexId = 456;

    final byte[] key = null;
    final ORID value = new ORecordId(12, 38);

    OCellBTreeMultiValuePutCO co = new OCellBTreeMultiValuePutCO(encryptionName, keySerializerId, indexId, key, value);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int pos = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OCellBTreeMultiValuePutCO restoredCO = new OCellBTreeMultiValuePutCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertEquals(value, restoredCO.getValue());
  }
}
