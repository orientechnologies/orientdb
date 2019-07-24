package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OIndexEngineCreateCOSerializationTest {
  @Test
  public void serializationTest() {
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

    final int indexId = 12;

    OIndexEngineCreateCO indexEngineCreateCO = new OIndexEngineCreateCO(engineName, algorithm, indexType, keySerializerId,
        valueSerializerId, isAutomatic, version, apiVersion, multiValue, engineProperties, keySize, keyTypes, nullValuesSupport,
        indexId);

    indexEngineCreateCO.setOperationUnitId(operationUnitId);

    final int size = indexEngineCreateCO.serializedSize();
    byte[] stream = new byte[size + 1];

    int pos = indexEngineCreateCO.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OIndexEngineCreateCO restoredIndexEngineCreateCO = new OIndexEngineCreateCO();
    pos = restoredIndexEngineCreateCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredIndexEngineCreateCO.getOperationUnitId());
    Assert.assertEquals(engineName, restoredIndexEngineCreateCO.getEngineName());
    Assert.assertEquals(algorithm, restoredIndexEngineCreateCO.getAlgorithm());
    Assert.assertEquals(indexType, restoredIndexEngineCreateCO.getIndexType());
    Assert.assertEquals(keySerializerId, restoredIndexEngineCreateCO.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredIndexEngineCreateCO.getValueSerializerId());
    Assert.assertEquals(isAutomatic, restoredIndexEngineCreateCO.isAutomatic());
    Assert.assertEquals(version, restoredIndexEngineCreateCO.getVersion());
    Assert.assertEquals(apiVersion, restoredIndexEngineCreateCO.getApiVersion());
    Assert.assertEquals(multiValue, restoredIndexEngineCreateCO.isMultiValue());
    Assert.assertEquals(engineProperties, restoredIndexEngineCreateCO.getEngineProperties());
    Assert.assertEquals(indexId, restoredIndexEngineCreateCO.getIndexId());

    Assert.assertEquals(keySize, restoredIndexEngineCreateCO.getKeySize());
    Assert.assertArrayEquals(keyTypes, restoredIndexEngineCreateCO.getKeyTypes());

    Assert.assertEquals(nullValuesSupport, restoredIndexEngineCreateCO.isNullValuesSupport());
  }

  @Test
  public void serializationNullTest() {
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

    final int indexId = 12;

    OIndexEngineCreateCO indexEngineCreateCO = new OIndexEngineCreateCO(engineName, algorithm, indexType, keySerializerId,
        valueSerializerId, isAutomatic, version, apiVersion, multiValue, engineProperties, keySize, keyTypes, nullValuesSupport,
        indexId);

    indexEngineCreateCO.setOperationUnitId(operationUnitId);

    final int size = indexEngineCreateCO.serializedSize();
    byte[] stream = new byte[size + 1];

    int pos = indexEngineCreateCO.toStream(stream, 1);
    Assert.assertEquals(size + 1, pos);

    OIndexEngineCreateCO restoredIndexEngineCreateCO = new OIndexEngineCreateCO();
    pos = restoredIndexEngineCreateCO.fromStream(stream, 1);

    Assert.assertEquals(size + 1, pos);
    Assert.assertEquals(operationUnitId, restoredIndexEngineCreateCO.getOperationUnitId());
    Assert.assertEquals(engineName, restoredIndexEngineCreateCO.getEngineName());
    Assert.assertEquals(algorithm, restoredIndexEngineCreateCO.getAlgorithm());
    Assert.assertEquals(indexType, restoredIndexEngineCreateCO.getIndexType());
    Assert.assertEquals(keySerializerId, restoredIndexEngineCreateCO.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredIndexEngineCreateCO.getValueSerializerId());
    Assert.assertEquals(isAutomatic, restoredIndexEngineCreateCO.isAutomatic());
    Assert.assertEquals(version, restoredIndexEngineCreateCO.getVersion());
    Assert.assertEquals(apiVersion, restoredIndexEngineCreateCO.getApiVersion());
    Assert.assertEquals(multiValue, restoredIndexEngineCreateCO.isMultiValue());
    Assert.assertEquals(indexId, restoredIndexEngineCreateCO.getIndexId());

    Assert.assertEquals(Collections.emptyMap(), restoredIndexEngineCreateCO.getEngineProperties());

    Assert.assertEquals(keySize, restoredIndexEngineCreateCO.getKeySize());
    Assert.assertArrayEquals(new OType[0], restoredIndexEngineCreateCO.getKeyTypes());

    Assert.assertEquals(nullValuesSupport, restoredIndexEngineCreateCO.isNullValuesSupport());
  }
}
