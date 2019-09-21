package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.sbtree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine.OAbstractIndexCO;

import java.nio.ByteBuffer;

public class OSBTreeRemoveCO extends OAbstractIndexCO {
  private byte[] value;
  private byte   valueSerializerId;

  public OSBTreeRemoveCO() {
  }

  public OSBTreeRemoveCO(final int indexId, final String encryptionName, final byte keySerializerId, final byte[] key,
      final byte[] value, final byte valueSerializerId) {
    super(indexId, encryptionName, keySerializerId, key);

    this.value = value;
    this.valueSerializerId = valueSerializerId;
  }

  public byte[] getValue() {
    return value;
  }

  public byte getValueSerializerId() {
    return valueSerializerId;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) {
    final Object key = deserializeKey(storage);

    try {
      storage.removeKeyFromIndexInternal(indexId, key);
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not redo operation for index with id " + indexId), e);
    }
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) {
    final Object key = deserializeKey(storage);
    final Object value = deserializeValue();

    try {
      storage.putIndexValueInternal(indexId, key, value);
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not undo operation for index with id " + indexId), e);
    }
  }

  private Object deserializeValue() {
    final OBinarySerializer valueSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(valueSerializerId);
    return valueSerializer.deserializeNativeObject(value, 0);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(valueSerializerId);

    buffer.putInt(value.length);
    buffer.put(value);
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    valueSerializerId = buffer.get();

    final int valueLen = buffer.getInt();
    value = new byte[valueLen];
    buffer.get(value);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + value.length;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.SBTREE_REMOVE_CO;
  }
}
