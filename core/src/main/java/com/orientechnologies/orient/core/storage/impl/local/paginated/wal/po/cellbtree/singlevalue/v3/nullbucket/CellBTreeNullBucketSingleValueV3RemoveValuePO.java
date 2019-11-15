package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.singlevalue.v3.nullbucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3.CellBTreeSingleValueV3NullBucket;

import java.nio.ByteBuffer;

public final class CellBTreeNullBucketSingleValueV3RemoveValuePO extends PageOperationRecord {
  private ORID value;

  public CellBTreeNullBucketSingleValueV3RemoveValuePO() {
  }

  public CellBTreeNullBucketSingleValueV3RemoveValuePO(ORID value) {
    this.value = value;
  }

  public ORID getValue() {
    return value;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(cacheEntry);
    bucket.removeValue();
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeSingleValueV3NullBucket bucket = new CellBTreeSingleValueV3NullBucket(cacheEntry);
    bucket.setValue(value);
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_SINGLE_VALUE_V3_REMOVE_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
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

    value = new ORecordId(clusterId, clusterPosition);
  }
}
