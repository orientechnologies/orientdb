package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

final class OEntryPoint<K> extends ODurablePage {
  private static final int KEY_SERIALIZER_OFFSET = NEXT_FREE_POSITION;
  private static final int KEY_SIZE_OFFSET       = KEY_SERIALIZER_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int TREE_SIZE_OFFSET      = KEY_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int PAGES_SIZE_OFFSET     = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  private final String  encryptionName;
  private final OType[] keyTypes;
  private final boolean dataInitialized;

  OEntryPoint(final OCacheEntry cacheEntry, final OBinarySerializer<K> keySerializer, final OType[] keyTypes, final int keySize,
      final OEncryption encryption) {
    super(cacheEntry);

    dataInitialized = true;
    if (encryption == null) {
      encryptionName = null;
    } else {
      encryptionName = encryption.name();
    }

    this.keyTypes = keyTypes;

    setByteValue(KEY_SERIALIZER_OFFSET, keySerializer.getId());
    setIntValue(KEY_SIZE_OFFSET, keySize);
    setLongValue(TREE_SIZE_OFFSET, 0);

    int offset = PAGES_SIZE_OFFSET + OLongSerializer.LONG_SIZE;

    if (encryptionName == null) {
      setByteValue(offset, (byte) 0);
      offset++;
    } else {
      setByteValue(offset, (byte) 1);
      offset++;

      final int encryptionNameSize = OStringSerializer.INSTANCE.getObjectSize(encryptionName);
      final byte[] rawEncryptionName = new byte[encryptionNameSize];
      OStringSerializer.INSTANCE.serializeNativeObject(encryptionName, rawEncryptionName, 0);
      setBinaryValue(offset, rawEncryptionName);
      offset += encryptionNameSize;
    }

    if (keyTypes == null) {
      setByteValue(offset, (byte) 0);
    } else {
      setByteValue(offset, (byte) 1);
      offset++;

      setIntValue(offset, keyTypes.length);
      offset += OIntegerSerializer.INT_SIZE;

      for (OType keyType : keyTypes) {
        setByteValue(offset, (byte) keyType.getId());
        offset += OByteSerializer.BYTE_SIZE;
      }
    }
  }

  OEntryPoint(final OCacheEntry cacheEntry, boolean readAllData) {
    super(cacheEntry);

    if (readAllData) {
      dataInitialized = true;

      int offset = PAGES_SIZE_OFFSET + OLongSerializer.LONG_SIZE;
      final boolean encryptionIsPresent = (getByteValue(offset) == 1);
      offset++;

      if (encryptionIsPresent) {
        final int encryptionSize = getObjectSizeInDirectMemory(OStringSerializer.INSTANCE, offset);
        encryptionName = deserializeFromDirectMemory(OStringSerializer.INSTANCE, offset);
        offset += encryptionSize;
      } else {
        encryptionName = null;
      }

      final boolean keyTypesArePresent = getByteValue(offset) == 1;
      if (keyTypesArePresent) {
        offset++;

        final int keyTypesSize = getIntValue(offset);
        offset += OIntegerSerializer.INT_SIZE;

        keyTypes = new OType[keyTypesSize];
        for (int i = 0; i < keyTypes.length; i++) {
          final byte typeId = getByteValue(offset);
          offset += OByteSerializer.BYTE_SIZE;

          keyTypes[i] = OType.getById(typeId);
        }
      } else {
        keyTypes = null;
      }
    } else {
      dataInitialized = false;

      encryptionName = null;
      keyTypes = null;
    }

  }

  OEntryPoint(final OCacheEntry cacheEntry) {
    this(cacheEntry, true);
  }

  void setTreeSize(long size) {
    setLongValue(TREE_SIZE_OFFSET, size);
  }

  long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  void setPagesSize(int pages) {
    setIntValue(PAGES_SIZE_OFFSET, pages);
  }

  int getPagesSize() {
    return getIntValue(PAGES_SIZE_OFFSET);
  }

  String getEncryptionName() {
    if (!dataInitialized) {
      throw new IllegalStateException("Data were not read");
    }

    return encryptionName;
  }

  OType[] getKeyTypes() {
    if (!dataInitialized) {
      throw new IllegalStateException("Data were not read");
    }

    return keyTypes;
  }

  int getKeySize() {
    return getIntValue(KEY_SIZE_OFFSET);
  }

  byte getKeySerializerId() {
    return getByteValue(KEY_SERIALIZER_OFFSET);
  }
}
