package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class OCellBTreeSingleValueRemoveCOSerializationTest {
  @Test
  public void serializationTest() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte keySerializerId = 23;
    final int indexId = 123;
    final String encryptionName = "encryption";

    final Random random = new Random();

    final byte[] key = new byte[23];
    random.nextBytes(key);

    final ORID prevValue = new ORecordId(12, 38);

    OCellBTreeSingleValueRemoveCO co = new OCellBTreeSingleValueRemoveCO(keySerializerId, indexId, encryptionName, key, prevValue);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int pos = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OCellBTreeSingleValueRemoveCO restoredCO = new OCellBTreeSingleValueRemoveCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertArrayEquals(key, restoredCO.getKey());
    Assert.assertEquals(prevValue, restoredCO.getPrevValue());
  }

  @Test
  public void serializationNullTest() {
    OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final byte keySerializerId = 23;
    final int indexId = 123;
    final String encryptionName = null;

    final Random random = new Random();

    final byte[] key = null;

    final ORID prevValue = new ORecordId(12, 38);

    OCellBTreeSingleValueRemoveCO co = new OCellBTreeSingleValueRemoveCO(keySerializerId, indexId, encryptionName, key, prevValue);
    co.setOperationUnitId(operationUnitId);

    final int size = co.serializedSize();
    final byte[] stream = new byte[size + 1];

    int pos = co.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OCellBTreeSingleValueRemoveCO restoredCO = new OCellBTreeSingleValueRemoveCO();
    pos = restoredCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredCO.getOperationUnitId());
    Assert.assertEquals(indexId, restoredCO.getIndexId());
    Assert.assertEquals(encryptionName, restoredCO.getEncryptionName());
    Assert.assertEquals(prevValue, restoredCO.getPrevValue());
    Assert.assertArrayEquals(key, restoredCO.getKey());
  }
}
