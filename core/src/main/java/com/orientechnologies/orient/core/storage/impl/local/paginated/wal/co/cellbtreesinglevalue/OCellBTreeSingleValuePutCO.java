package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine.OAbstractIndexCO;

import java.io.IOException;
import java.nio.ByteBuffer;

public class OCellBTreeSingleValuePutCO extends OAbstractIndexCO {
  private ORID value;
  private ORID oldValue;

  public OCellBTreeSingleValuePutCO() {
  }

  public OCellBTreeSingleValuePutCO(final byte[] key, final ORID value, final ORID oldValue, final byte keySerializerId,
      final int indexId, final String encryptionName) {
    super(indexId, encryptionName, keySerializerId, key);

    this.value = value;
    this.oldValue = oldValue;
  }

  public ORID getValue() {
    return value;
  }

  public ORID getOldValue() {
    return oldValue;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    final Object deserializedKey = deserializeKey(storage);

    try {
      storage.putRidIndexEntryInternal(indexId, deserializedKey, value);
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not redo operation for index with id " + indexId), e);
    }
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    final Object deserializedKey = deserializeKey(storage);

    try {
      if (oldValue == null) {
        storage.removeKeyFromIndexInternal(indexId, deserializedKey);
      } else {
        storage.putRidIndexEntryInternal(indexId, deserializedKey, oldValue);
      }
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not undo operation for index with id " + indexId), e);
    }
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putShort((short) value.getClusterId());
    buffer.putLong(value.getClusterPosition());

    if (oldValue == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);
      buffer.putShort((short) oldValue.getClusterId());
      buffer.putLong(oldValue.getClusterPosition());
    }
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int clusterId = buffer.getShort();
    final long clusterPosition = buffer.getLong();

    this.value = new ORecordId(clusterId, clusterPosition);

    if (buffer.get() == 1) {
      final int oldClusterId = buffer.getShort();
      final long oldClusterPosition = buffer.getLong();

      this.oldValue = new ORecordId(oldClusterId, oldClusterPosition);
    }
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CELL_BTREE_SINGLE_VALUE_PUT_CO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE + (
        oldValue != null ? (OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE) : 0);
  }
}
