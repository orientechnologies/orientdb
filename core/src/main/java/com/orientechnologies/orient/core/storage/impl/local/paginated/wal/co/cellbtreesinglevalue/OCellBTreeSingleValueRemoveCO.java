package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreesinglevalue;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.exception.OInvalidIndexEngineIdException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.indexengine.OAbstractIndexCO;

import java.nio.ByteBuffer;

public class OCellBTreeSingleValueRemoveCO extends OAbstractIndexCO {
  private ORID prevValue;

  public OCellBTreeSingleValueRemoveCO() {
  }

  public OCellBTreeSingleValueRemoveCO(final byte keySerializerId, final int indexId, final String encryptionName, final byte[] key,
      final ORID prevValue) {
    super(indexId, encryptionName, keySerializerId, key);

    this.prevValue = prevValue;
  }

  public ORID getPrevValue() {
    return prevValue;
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

    try {
      storage.putRidIndexEntryInternal(indexId, key, prevValue);
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not undo operation for index with id " + indexId), e);
    }
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putShort((short) prevValue.getClusterId());
    buffer.putLong(prevValue.getClusterPosition());
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int clusterId = buffer.getShort();
    final long clusterPosition = buffer.getLong();

    this.prevValue = new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CELL_BTREE_SINGLE_VALUE_REMOVE_CO;
  }
}
