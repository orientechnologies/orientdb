package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.localhashtable;

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

public class OLocalHashTablePutCO extends OAbstractIndexCO {
  private byte   valueSerializerId;
  private byte[] value;

  private byte[] prevValue;

  public OLocalHashTablePutCO() {
  }

  public OLocalHashTablePutCO(final int indexId, final String encryptionName, final byte keySerializerId, final byte[] key,
      final byte valueSerializerId, final byte[] value, final byte[] prevValue) {
    super(indexId, encryptionName, keySerializerId, key);
    this.valueSerializerId = valueSerializerId;
    this.value = value;
    this.prevValue = prevValue;
  }

  public byte getValueSerializerId() {
    return valueSerializerId;
  }

  public byte[] getValue() {
    return value;
  }

  public byte[] getPrevValue() {
    return prevValue;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) {
    final Object key = deserializeKey(storage);
    final Object value = deserializeValue();

    try {
      storage.putIndexValueInternal(indexId, key, value);
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not redo operation for index with id " + indexId), e);
    }
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) {
    final Object key = deserializeKey(storage);

    try {
      if (prevValue == null) {
        storage.removeKeyFromIndexInternal(indexId, key);
      } else {

        storage.putIndexValueInternal(indexId, key, deserializePrevValue());
      }
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not undo operation for index with id " + indexId), e);
    }
  }

  private Object deserializeValue() {
    final OBinarySerializer valueSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(valueSerializerId);
    return valueSerializer.deserializeNativeObject(value, 0);
  }

  private Object deserializePrevValue() {
    final OBinarySerializer valueSerializer = OBinarySerializerFactory.getInstance().getObjectSerializer(valueSerializerId);
    return valueSerializer.deserializeNativeObject(prevValue, 0);
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.put(valueSerializerId);

    buffer.putInt(value.length);
    buffer.put(value);

    if (prevValue == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      buffer.putInt(prevValue.length);
      buffer.put(prevValue);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    valueSerializerId = buffer.get();

    final int valueLen = buffer.getInt();

    this.value = new byte[valueLen];
    buffer.get(this.value);

    if (buffer.get() > 0) {
      final int prevValueLen = buffer.getInt();
      prevValue = new byte[prevValueLen];
      buffer.get(prevValue);
    }
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE + value.length + (
        prevValue != null ? (OIntegerSerializer.INT_SIZE + prevValue.length) : 0);
  }

  @Override
  public byte getId() {
    return WALRecordTypes.LOCAL_HASHTABLE_PUT_CO;
  }
}
