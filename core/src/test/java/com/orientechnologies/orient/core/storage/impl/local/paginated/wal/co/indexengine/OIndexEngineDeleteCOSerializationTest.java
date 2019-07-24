package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OIndexEngineDeleteCOSerializationTest {
  @Test
  public void testSerialization() {
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final String engineName = "testEngine";
    final String algorithm = "tree";
    final String indexType = "unique";

    final byte keySerializerId = 2;
    final byte valueSerializerId = 5;
    final boolean isAutomatic = true;
    final int version = 23;
    final int apiVersion = 54;
    final boolean multiValue = false;

    final Map<String, String> engineProperties = new HashMap<>();
    engineProperties.put("prop1", "value1");
    engineProperties.put("prop2", "value2");

    final int keySize = 3;

    final OType[] keyTypes = new OType[2];
    keyTypes[0] = OType.BINARY;
    keyTypes[1] = OType.BYTE;

    final boolean nullValuesSupport = true;

    final int indexId = 42;

    OIndexEngineDeleteCO indexEngineDeleteCO = new OIndexEngineDeleteCO(indexId, engineName, algorithm, indexType, keySerializerId,
        valueSerializerId, isAutomatic, version, apiVersion, multiValue, engineProperties, keySize, keyTypes, nullValuesSupport);
    indexEngineDeleteCO.setOperationUnitId(operationUnitId);

    final int size = indexEngineDeleteCO.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = indexEngineDeleteCO.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OIndexEngineDeleteCO restoredIndexEngineDeleteCO = new OIndexEngineDeleteCO();
    pos = restoredIndexEngineDeleteCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredIndexEngineDeleteCO.getOperationUnitId());
    Assert.assertEquals(indexId, restoredIndexEngineDeleteCO.getIndexId());
    Assert.assertEquals(engineName, restoredIndexEngineDeleteCO.getEngineName());
    Assert.assertEquals(algorithm, restoredIndexEngineDeleteCO.getAlgorithm());
    Assert.assertEquals(indexType, restoredIndexEngineDeleteCO.getIndexType());
    Assert.assertEquals(keySerializerId, restoredIndexEngineDeleteCO.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredIndexEngineDeleteCO.getValueSerializerId());
    Assert.assertEquals(isAutomatic, restoredIndexEngineDeleteCO.isAutomatic());
    Assert.assertEquals(version, restoredIndexEngineDeleteCO.getVersion());
    Assert.assertEquals(apiVersion, restoredIndexEngineDeleteCO.getApiVersion());
    Assert.assertEquals(multiValue, restoredIndexEngineDeleteCO.isMultiValue());
    Assert.assertEquals(engineProperties, restoredIndexEngineDeleteCO.getEngineProperties());
    Assert.assertEquals(keySize, restoredIndexEngineDeleteCO.getKeySize());
    Assert.assertEquals(nullValuesSupport, restoredIndexEngineDeleteCO.isNullValuesSupport());
    Assert.assertArrayEquals(keyTypes, restoredIndexEngineDeleteCO.getKeyTypes());
  }

  @Test
  public void testNullSerialization() {
    final OOperationUnitId operationUnitId = OOperationUnitId.generateId();

    final String engineName = "testEngine";
    final String algorithm = "tree";
    final String indexType = "unique";

    final byte keySerializerId = 2;
    final byte valueSerializerId = 5;
    final boolean isAutomatic = true;
    final int version = 23;
    final int apiVersion = 54;
    final boolean multiValue = false;

    final Map<String, String> engineProperties = null;

    final int keySize = 3;

    final OType[] keyTypes = null;

    final boolean nullValuesSupport = true;

    final int indexId = 42;

    OIndexEngineDeleteCO indexEngineDeleteCO = new OIndexEngineDeleteCO(indexId, engineName, algorithm, indexType, keySerializerId,
        valueSerializerId, isAutomatic, version, apiVersion, multiValue, engineProperties, keySize, keyTypes, nullValuesSupport);
    indexEngineDeleteCO.setOperationUnitId(operationUnitId);

    final int size = indexEngineDeleteCO.serializedSize();
    final byte[] stream = new byte[size + 1];
    int pos = indexEngineDeleteCO.toStream(stream, 1);

    Assert.assertEquals(size + 1, pos);

    OIndexEngineDeleteCO restoredIndexEngineDeleteCO = new OIndexEngineDeleteCO();
    pos = restoredIndexEngineDeleteCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredIndexEngineDeleteCO.getOperationUnitId());
    Assert.assertEquals(indexId, restoredIndexEngineDeleteCO.getIndexId());
    Assert.assertEquals(engineName, restoredIndexEngineDeleteCO.getEngineName());
    Assert.assertEquals(algorithm, restoredIndexEngineDeleteCO.getAlgorithm());
    Assert.assertEquals(indexType, restoredIndexEngineDeleteCO.getIndexType());
    Assert.assertEquals(keySerializerId, restoredIndexEngineDeleteCO.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredIndexEngineDeleteCO.getValueSerializerId());
    Assert.assertEquals(isAutomatic, restoredIndexEngineDeleteCO.isAutomatic());
    Assert.assertEquals(version, restoredIndexEngineDeleteCO.getVersion());
    Assert.assertEquals(apiVersion, restoredIndexEngineDeleteCO.getApiVersion());
    Assert.assertEquals(multiValue, restoredIndexEngineDeleteCO.isMultiValue());
    Assert.assertEquals(Collections.emptyMap(), restoredIndexEngineDeleteCO.getEngineProperties());
    Assert.assertEquals(keySize, restoredIndexEngineDeleteCO.getKeySize());
    Assert.assertEquals(nullValuesSupport, restoredIndexEngineDeleteCO.isNullValuesSupport());
    Assert.assertArrayEquals(new OType[0], restoredIndexEngineDeleteCO.getKeyTypes());
  }
}
