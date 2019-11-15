package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v1.nullbucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v1.CellBTreeNullBucketSingleValueV1;

import java.nio.ByteBuffer;

public final class CellBTreeNullBucketSingleValueV1SetValuePO extends PageOperationRecord {
  private ORID prevValue;
  private ORID value;

  public CellBTreeNullBucketSingleValueV1SetValuePO() {
  }

  public CellBTreeNullBucketSingleValueV1SetValuePO(ORID prevValue, ORID value) {
    this.prevValue = prevValue;
    this.value = value;
  }

  public ORID getPrevValue() {
    return prevValue;
  }

  public ORID getValue() {
    return value;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeNullBucketSingleValueV1 bucket = new CellBTreeNullBucketSingleValueV1(cacheEntry);
    bucket.setValue(value);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeNullBucketSingleValueV1 bucket = new CellBTreeNullBucketSingleValueV1(cacheEntry);
    if (prevValue == null) {
      bucket.removeValue();
    } else {
      bucket.setValue(prevValue);
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V1_SET_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    int size = OByteSerializer.BYTE_SIZE + OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
    if (prevValue != null) {
      size += OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
    }

    return super.serializedSize() + size;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putShort((short) value.getClusterId());
    buffer.putLong(value.getClusterPosition());

    if (prevValue != null) {
      buffer.put((byte) 1);

      buffer.putShort((short) prevValue.getClusterId());
      buffer.putLong(prevValue.getClusterPosition());
    } else {
      buffer.put((byte) 0);
    }
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    int clusterId = buffer.getShort();
    long clusterPosition = buffer.getLong();

    value = new ORecordId(clusterId, clusterPosition);
    if (buffer.get() > 0) {
      clusterId = buffer.getShort();
      clusterPosition = buffer.getLong();

      prevValue = new ORecordId(clusterId, clusterPosition);
    }
  }
}
