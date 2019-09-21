package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.co.cellbtreemultivalue;

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

import java.io.IOException;
import java.nio.ByteBuffer;

public class OCellBtreeMultiValueRemoveEntryCO extends OAbstractIndexCO {
  private ORID value;

  public OCellBtreeMultiValueRemoveEntryCO() {
  }

  public OCellBtreeMultiValueRemoveEntryCO(final int indexId, final byte keySerializerId, final String encryptionName,
      final byte[] key, final ORID value) {
    super(indexId, encryptionName, keySerializerId, key);

    this.value = value;
  }

  public ORID getValue() {
    return value;
  }

  @Override
  public void redo(final OAbstractPaginatedStorage storage) throws IOException {
    final Object key = deserializeKey(storage);

    try {
      storage.removeRidIndexEntryInternal(indexId, key, value);
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not redo operation for index with id " + indexId), e);
    }
  }

  @Override
  public void undo(final OAbstractPaginatedStorage storage) throws IOException {
    final Object key = deserializeKey(storage);

    try {
      storage.putRidIndexEntryInternal(indexId, key, value);
    } catch (OInvalidIndexEngineIdException e) {
      throw OException.wrapException(new OStorageException("Can not redo operation for index with id " + indexId), e);
    }
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putShort((short) value.getClusterId());
    buffer.putLong(value.getClusterPosition());
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int clusterId = buffer.getShort();
    final long clusterPosition = buffer.getLong();

    this.value = new ORecordId(clusterId, clusterPosition);
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.CELL_BTREE_MULTI_VALUE_REMOVE_ENTRY_CO;
  }
}
