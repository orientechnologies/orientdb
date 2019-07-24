package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OCellBTreeSingleValuePutCOSerializationTest {
  @Test
  public void testSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();

    final byte[] key = new byte[12];
    random.nextBytes(key);

    final ORID value = new ORecordId(12, 38);
    final ORID oldValue = new ORecordId(34, 56);

    final byte keySerializerId = 12;

    final int indexId = 45;
    final String encryptionName = "encryption";

    OCellBTreeSingleValuePutCO co = new OCellBTreeSingleValuePutCO(key, value, oldValue, keySerializerId, indexId, encryptionName);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int position = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, position);

    OCellBTreeSingleValuePutCO restoredCO = new OCellBTreeSingleValuePutCO();
    position = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, position);

    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertEquals(value, restoredCO.getValue());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertEquals(oldValue, restoredCO.getOldValue());
  }

  @Test
  public void testNullSerialization() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final Random random = new Random();

    final byte[] key = null;

    final ORID value = new ORecordId(12, 38);
    final ORID oldValue = null;

    final byte keySerializerId = 12;

    final int indexId = 45;
    final String encryptionName = null;

    OCellBTreeSingleValuePutCO co = new OCellBTreeSingleValuePutCO(key, value, oldValue, keySerializerId, indexId, encryptionName);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int position = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, position);

    OCellBTreeSingleValuePutCO restoredCO = new OCellBTreeSingleValuePutCO();
    position = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, position);

    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());

    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertEquals(value, restoredCO.getValue());
    Assert.assertEquals(keySerializerId, restoredCO.getKeySerializerId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertEquals(oldValue, restoredCO.getOldValue());
  }
}
