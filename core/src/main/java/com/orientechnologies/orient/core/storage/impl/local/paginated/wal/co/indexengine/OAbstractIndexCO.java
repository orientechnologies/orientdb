package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.OComponentOperationRecord;

import java.nio.ByteBuffer;

public abstract class OAbstractIndexCO extends OComponentOperationRecord {
  private String encryptionName;
  private byte   keySerializerId;

  protected byte[] key;
  protected int    indexId;

  public OAbstractIndexCO() {
  }

  public OAbstractIndexCO(final int indexId, final String encryptionName, final byte keySerializerId, final byte[] key) {
    this.indexId = indexId;
    this.encryptionName = encryptionName;
    this.keySerializerId = keySerializerId;
    this.key = key;
  }

  public String getEncryptionName() {
    return encryptionName;
  }

  public byte getKeySerializerId() {
    return keySerializerId;
  }

  public byte[] getKey() {
    return key;
  }

  public int getIndexId() {
    return indexId;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    buffer.put(keySerializerId);
    buffer.putInt(indexId);

    if (encryptionName == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      OStringSerializer.INSTANCE.serializeInByteBufferObject(encryptionName, buffer);
    }

    if (key == null) {
      buffer.putInt(0);
    } else {
      buffer.putInt(key.length);
      buffer.put(key);
    }
  }

  protected Object deserializeKey(final OAbstractPaginatedStorage storage) {
    final OBinarySerializerFactory binarySerializerFactory = OBinarySerializerFactory.getInstance();
    final OBinarySerializer keySerializer = binarySerializerFactory.getObjectSerializer(keySerializerId);

    if (key == null) {
      return null;
    }

    if (encryptionName != null) {
      final OEncryptionFactory encryptionFactory = OEncryptionFactory.INSTANCE;
      final OStorageConfiguration storageConfiguration = storage.getConfiguration();
      final String encryptionKey = storageConfiguration.getContextConfiguration()
          .getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

      final OEncryption encryption = encryptionFactory.getEncryption(encryptionName, encryptionKey);
      final byte[] decryptedKey = encryption.decrypt(key, OIntegerSerializer.INT_SIZE, key.length - OIntegerSerializer.INT_SIZE);

      return keySerializer.deserializeNativeObject(decryptedKey, 0);
    }

    return keySerializer.deserializeNativeObject(key, 0);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    keySerializerId = buffer.get();
    indexId = buffer.getInt();

    if (buffer.get() != 0) {
      encryptionName = OStringSerializer.INSTANCE.deserializeFromByteBufferObject(buffer);
    }

    final int keyLen = buffer.getInt();
    if (keyLen > 0) {
      this.key = new byte[keyLen];
      buffer.get(this.key);
    }
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OByteSerializer.BYTE_SIZE + 2 * OIntegerSerializer.INT_SIZE + (encryptionName != null
        ? OStringSerializer.INSTANCE.getObjectSize(encryptionName)
        : 0) + (key != null ? key.length : 0);
  }
}
