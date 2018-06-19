package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitId;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.OLocalHashTableV2;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class OCreateHashTableOperation extends OLocalHashTableOperation {
  private long fileId;
  private long directoryFileId;

  private String metadataConfigurationFileExtension;
  private String treeStateFileExtension;
  private String bucketFileExtension;
  private String nullBucketFileExtension;

  private byte keySerializerId;
  private byte valueSerializerId;

  private boolean nullKeyIsSupported;
  private OType[] keyTypes;
  private String  encryptionName;
  private String  encryptionOptions;

  public OCreateHashTableOperation() {
  }

  public OCreateHashTableOperation(final OOperationUnitId operationUnitId, final String name,
      final String metadataConfigurationFileExtension, final String treeStateFileExtension, final String bucketFileExtension,
      final String nullBucketFileExtension, final long fileId, final long directoryFileId, final byte keySerializerId,
      final byte valueSerializerId, final boolean nullKeyIsSupported, final OType[] keyTypes, final String encryptionName,
      final String encryptionOptions) {
    super(operationUnitId, name);
    this.fileId = fileId;

    this.metadataConfigurationFileExtension = metadataConfigurationFileExtension;
    this.treeStateFileExtension = treeStateFileExtension;
    this.bucketFileExtension = bucketFileExtension;
    this.nullBucketFileExtension = nullBucketFileExtension;

    this.directoryFileId = directoryFileId;
    this.keySerializerId = keySerializerId;
    this.valueSerializerId = valueSerializerId;
    this.nullKeyIsSupported = nullKeyIsSupported;
    this.keyTypes = keyTypes;
    this.encryptionName = encryptionName;
    this.encryptionOptions = encryptionOptions;
  }

  public long getFileId() {
    return fileId;
  }

  long getDirectoryFileId() {
    return directoryFileId;
  }

  public String getMetadataConfigurationFileExtension() {
    return metadataConfigurationFileExtension;
  }

  public String getTreeStateFileExtension() {
    return treeStateFileExtension;
  }

  public String getBucketFileExtension() {
    return bucketFileExtension;
  }

  public String getNullBucketFileExtension() {
    return nullBucketFileExtension;
  }

  public byte getKeySerializerId() {
    return keySerializerId;
  }

  public byte getValueSerializerId() {
    return valueSerializerId;
  }

  public boolean isNullKeyIsSupported() {
    return nullKeyIsSupported;
  }

  public OType[] getKeyTypes() {
    return keyTypes;
  }

  public String getEncryptionName() {
    return encryptionName;
  }

  public String getEncryptionOptions() {
    return encryptionOptions;
  }

  @Override
  public void rollbackOperation(final OLocalHashTableV2 hashTable, final OAtomicOperation atomicOperation) {
    hashTable.deleteRollback(atomicOperation);
  }

  @Override
  public int toStream(final byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OLongSerializer.INSTANCE.serializeNative(fileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OLongSerializer.INSTANCE.serializeNative(directoryFileId, content, offset);
    offset += OLongSerializer.LONG_SIZE;

    OStringSerializer.INSTANCE.serializeNativeObject(metadataConfigurationFileExtension, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(metadataConfigurationFileExtension);

    OStringSerializer.INSTANCE.serializeNativeObject(treeStateFileExtension, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(treeStateFileExtension);

    OStringSerializer.INSTANCE.serializeNativeObject(bucketFileExtension, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(bucketFileExtension);

    OStringSerializer.INSTANCE.serializeNativeObject(nullBucketFileExtension, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(nullBucketFileExtension);

    content[offset] = keySerializerId;
    offset++;

    content[offset] = valueSerializerId;
    offset++;

    content[offset] = nullKeyIsSupported ? (byte) 1 : 0;
    offset++;

    if (keyTypes == null) {
      content[offset] = 0;
      offset++;
    } else {
      content[offset] = 1;
      offset++;

      OIntegerSerializer.INSTANCE.serializeNative(keyTypes.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      for (final OType keyType : keyTypes) {
        content[offset] = (byte) keyType.getId();
        offset++;
      }
    }

    if (encryptionName != null) {
      content[offset] = 1;
      offset++;

      OStringSerializer.INSTANCE.serializeNativeObject(encryptionName, content, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(encryptionName);

      if (encryptionOptions != null) {
        content[offset] = 1;
        offset++;

        OStringSerializer.INSTANCE.serializeNativeObject(encryptionOptions, content, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(encryptionOptions);
      } else {
        content[offset] = 0;
        offset++;
      }
    } else {
      content[offset] = 0;
      offset++;
    }

    return offset;
  }

  @Override
  public int fromStream(final byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    directoryFileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OLongSerializer.LONG_SIZE;

    metadataConfigurationFileExtension = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(metadataConfigurationFileExtension);

    treeStateFileExtension = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(treeStateFileExtension);

    bucketFileExtension = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(bucketFileExtension);

    nullBucketFileExtension = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(nullBucketFileExtension);

    keySerializerId = content[offset];
    offset++;

    valueSerializerId = content[offset];
    offset++;

    nullKeyIsSupported = content[offset] == 1;
    offset++;

    if (content[offset] == 0) {
      keyTypes = null;
      offset++;
    } else {
      offset++;

      final int keyTypesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      keyTypes = new OType[keyTypesSize];
      for (int i = 0; i < keyTypesSize; i++) {
        keyTypes[i] = OType.getById(content[offset]);
        offset++;
      }
    }

    if (content[offset] == 0) {
      offset++;
      encryptionName = null;
      encryptionOptions = null;
    } else {
      offset++;

      encryptionName = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
      offset += OStringSerializer.INSTANCE.getObjectSize(encryptionName);

      if (content[offset] == 0) {
        offset++;

        encryptionOptions = null;
      } else {
        offset++;

        encryptionOptions = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
        offset += OStringSerializer.INSTANCE.getObjectSize(encryptionOptions);
      }
    }

    return offset;
  }

  @Override
  public void toStream(final ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putLong(fileId);
    buffer.putLong(directoryFileId);

    OStringSerializer.INSTANCE.serializeInByteBufferObject(metadataConfigurationFileExtension, buffer);
    OStringSerializer.INSTANCE.serializeInByteBufferObject(treeStateFileExtension, buffer);
    OStringSerializer.INSTANCE.serializeInByteBufferObject(bucketFileExtension, buffer);
    OStringSerializer.INSTANCE.serializeInByteBufferObject(nullBucketFileExtension, buffer);

    buffer.put(keySerializerId);
    buffer.put(valueSerializerId);
    buffer.put(nullKeyIsSupported ? (byte) 1 : 0);

    if (keyTypes == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);

      buffer.putInt(keyTypes.length);

      for (final OType keyType : keyTypes) {
        buffer.put((byte) keyType.getId());
      }
    }

    if (encryptionName != null) {
      buffer.put((byte) 1);

      OStringSerializer.INSTANCE.serializeInByteBufferObject(encryptionName, buffer);
      if (encryptionOptions != null) {
        buffer.put((byte) 1);
        OStringSerializer.INSTANCE.serializeInByteBufferObject(encryptionOptions, buffer);
      } else {
        buffer.put((byte) 0);
      }
    } else {
      buffer.put((byte) 0);
    }
  }

  @Override
  public int serializedSize() {
    int size = super.serializedSize();
    size += OLongSerializer.LONG_SIZE;
    size += OLongSerializer.LONG_SIZE;

    size += OStringSerializer.INSTANCE.getObjectSize(metadataConfigurationFileExtension);
    size += OStringSerializer.INSTANCE.getObjectSize(treeStateFileExtension);
    size += OStringSerializer.INSTANCE.getObjectSize(bucketFileExtension);
    size += OStringSerializer.INSTANCE.getObjectSize(nullBucketFileExtension);

    size += 3 * OByteSerializer.BYTE_SIZE;

    size += OByteSerializer.BYTE_SIZE;
    if (keyTypes != null) {
      size += OIntegerSerializer.INT_SIZE + keyTypes.length;
    }

    size += OByteSerializer.BYTE_SIZE;
    if (encryptionName != null) {
      size += OStringSerializer.INSTANCE.getObjectSize(encryptionName);

      size += OByteSerializer.BYTE_SIZE;
      if (encryptionOptions != null) {
        size += OStringSerializer.INSTANCE.getObjectSize(encryptionOptions);
      }
    }

    return size;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CREATE_HASH_TABLE_OPERATION;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    final OCreateHashTableOperation that = (OCreateHashTableOperation) o;
    return fileId == that.fileId && directoryFileId == that.directoryFileId && keySerializerId == that.keySerializerId
        && valueSerializerId == that.valueSerializerId && nullKeyIsSupported == that.nullKeyIsSupported && Objects
        .equals(metadataConfigurationFileExtension, that.metadataConfigurationFileExtension) && Objects
        .equals(treeStateFileExtension, that.treeStateFileExtension) && Objects
        .equals(bucketFileExtension, that.bucketFileExtension) && Objects
        .equals(nullBucketFileExtension, that.nullBucketFileExtension) && Arrays.equals(keyTypes, that.keyTypes) && Objects
        .equals(encryptionName, that.encryptionName) && Objects.equals(encryptionOptions, that.encryptionOptions);
  }

  @Override
  public int hashCode() {

    int result = Objects.hash(super.hashCode(), fileId, directoryFileId, metadataConfigurationFileExtension, treeStateFileExtension,
        bucketFileExtension, nullBucketFileExtension, keySerializerId, valueSerializerId, nullKeyIsSupported, encryptionName,
        encryptionOptions);
    result = 31 * result + Arrays.hashCode(keyTypes);
    return result;
  }

  @Override
  public String toString() {
    return toString("fileId=" + fileId + ", directoryFileId=" + directoryFileId + ", metadataConfigurationFileExtension='"
        + metadataConfigurationFileExtension + '\'' + ", treeStateFileExtension='" + treeStateFileExtension + '\''
        + ", bucketFileExtension='" + bucketFileExtension + '\'' + ", nullBucketFileExtension='" + nullBucketFileExtension + '\''
        + ", keySerializerId=" + keySerializerId + ", valueSerializerId=" + valueSerializerId + ", nullKeyIsSupported="
        + nullKeyIsSupported + ", keyTypes=" + Arrays.toString(keyTypes) + ", encryptionName='" + encryptionName + '\''
        + ", encryptionOptions='" + encryptionOptions + '\'');
  }
}
