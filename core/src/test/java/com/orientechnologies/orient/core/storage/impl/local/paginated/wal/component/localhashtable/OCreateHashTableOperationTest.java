package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OCreateHashTableOperationTest {
  @Test
  public void testSerializationArray() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = new OType[2];
    keyTypes[0] = OType.STRING;
    keyTypes[1] = OType.INTEGER;

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final String encryptionName = "pam";
    final String encryptionOptions = "param";

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);

    final int serializedSize = createHashTableOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createHashTableOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    offset = restoredCreateHashTableOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }

  @Test
  public void testSerializationArrayKeyTypesNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = null;

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final String encryptionName = "pam";
    final String encryptionOptions = "param";

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);

    final int serializedSize = createHashTableOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createHashTableOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    offset = restoredCreateHashTableOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }

  @Test
  public void testSerializationArrayEncryptionNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = new OType[2];
    keyTypes[0] = OType.STRING;
    keyTypes[1] = OType.INTEGER;

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final String encryptionName = null;
    final String encryptionOptions = null;

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);

    final int serializedSize = createHashTableOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createHashTableOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    offset = restoredCreateHashTableOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }

  @Test
  public void testSerializationArrayEncryptionOptionNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();
    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = new OType[2];
    keyTypes[0] = OType.STRING;
    keyTypes[1] = OType.INTEGER;

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final String encryptionName = "pam";
    final String encryptionOptions = null;

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);

    final int serializedSize = createHashTableOperation.serializedSize();
    final byte[] content = new byte[serializedSize + 1];
    int offset = createHashTableOperation.toStream(content, 1);
    Assert.assertEquals(content.length, offset);

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    offset = restoredCreateHashTableOperation.fromStream(content, 1);
    Assert.assertEquals(content.length, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }

  @Test
  public void testSerializationBuffer() {
    OOperationUnitId unitId = OOperationUnitId.generateId();

    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = new OType[2];
    keyTypes[0] = OType.STRING;
    keyTypes[1] = OType.INTEGER;

    final String encryptionName = "pam";
    final String encryptionOptions = "param";

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);
    final int serializedSize = createHashTableOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createHashTableOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    final int offset = restoredCreateHashTableOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }

  @Test
  public void testSerializationBufferKeyTypesNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();

    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = null;

    final String encryptionName = "pam";
    final String encryptionOptions = "param";

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);
    final int serializedSize = createHashTableOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createHashTableOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    final int offset = restoredCreateHashTableOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }

  @Test
  public void testSerializationBufferEncryptionNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();

    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = new OType[2];
    keyTypes[0] = OType.STRING;
    keyTypes[1] = OType.INTEGER;

    final String encryptionName = null;
    final String encryptionOptions = null;

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);
    final int serializedSize = createHashTableOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createHashTableOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    final int offset = restoredCreateHashTableOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }

  @Test
  public void testSerializationBufferEncryptionOptionNull() {
    OOperationUnitId unitId = OOperationUnitId.generateId();

    final byte keySerializerId = 12;
    final byte valueSerializerId = 34;
    final boolean nullKeyIsSupported = true;

    final OType[] keyTypes = new OType[2];
    keyTypes[0] = OType.STRING;
    keyTypes[1] = OType.INTEGER;

    final String encryptionName = "pam";
    final String encryptionOptions = null;

    String metadataConfigurationFileExtension = ".tdc";
    String treeStateFileExtension = ".tsf";
    String bucketFileExtension = ".tst";
    String nullBucketFileExtension = ".ghy";

    final OCreateHashTableOperation createHashTableOperation = new OCreateHashTableOperation(unitId, "caprica",
        metadataConfigurationFileExtension, treeStateFileExtension, bucketFileExtension, nullBucketFileExtension, 42, 24,
        keySerializerId, valueSerializerId, nullKeyIsSupported, keyTypes, encryptionName, encryptionOptions);
    final int serializedSize = createHashTableOperation.serializedSize();

    final ByteBuffer buffer = ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    createHashTableOperation.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    final OCreateHashTableOperation restoredCreateHashTableOperation = new OCreateHashTableOperation();
    final int offset = restoredCreateHashTableOperation.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, offset);
    Assert.assertEquals(createHashTableOperation, restoredCreateHashTableOperation);

    Assert.assertEquals(unitId, restoredCreateHashTableOperation.getOperationUnitId());
    Assert.assertEquals("caprica", restoredCreateHashTableOperation.getName());
    Assert.assertEquals(42, restoredCreateHashTableOperation.getFileId());
    Assert.assertEquals(24, restoredCreateHashTableOperation.getDirectoryFileId());
    Assert.assertEquals(keySerializerId, restoredCreateHashTableOperation.getKeySerializerId());
    Assert.assertEquals(valueSerializerId, restoredCreateHashTableOperation.getValueSerializerId());
    Assert.assertEquals(nullKeyIsSupported, restoredCreateHashTableOperation.isNullKeyIsSupported());
    Assert.assertArrayEquals(keyTypes, restoredCreateHashTableOperation.getKeyTypes());
    Assert.assertEquals(encryptionName, restoredCreateHashTableOperation.getEncryptionName());
    Assert.assertEquals(encryptionOptions, restoredCreateHashTableOperation.getEncryptionOptions());
    Assert
        .assertEquals(metadataConfigurationFileExtension, restoredCreateHashTableOperation.getMetadataConfigurationFileExtension());
    Assert.assertEquals(treeStateFileExtension, restoredCreateHashTableOperation.getTreeStateFileExtension());
    Assert.assertEquals(bucketFileExtension, restoredCreateHashTableOperation.getBucketFileExtension());
    Assert.assertEquals(nullBucketFileExtension, restoredCreateHashTableOperation.getNullBucketFileExtension());
  }


}
