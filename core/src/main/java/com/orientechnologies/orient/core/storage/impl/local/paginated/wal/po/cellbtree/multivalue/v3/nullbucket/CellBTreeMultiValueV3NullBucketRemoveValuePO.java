package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v3.nullbucket;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v3.CellBTreeMultiValueV3NullBucket;

import java.nio.ByteBuffer;

public class CellBTreeMultiValueV3NullBucketRemoveValuePO extends PageOperationRecord {
  private ORID rid;

  public CellBTreeMultiValueV3NullBucketRemoveValuePO() {
  }

  public CellBTreeMultiValueV3NullBucketRemoveValuePO(ORID rid) {
    this.rid = rid;
  }

  public ORID getRid() {
    return rid;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3NullBucket bucket = new CellBTreeMultiValueV3NullBucket(cacheEntry);
    final int result = bucket.removeValue(rid);
    if (result != 1) {
      throw new IllegalStateException("Can not redo remove value operation");
    }
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV3NullBucket bucket = new CellBTreeMultiValueV3NullBucket(cacheEntry);
    final long result = bucket.addValue(rid);
    if (result != -1) {
      throw new IllegalStateException("Can not undo remove value operation");
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_NULL_BUCKET_MULTI_VALUE_V3_REMOVE_VALUE_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(final ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putShort((short) rid.getClusterId());
    buffer.putLong(rid.getClusterPosition());
  }

  @Override
  protected void deserializeFromByteBuffer(final ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    final int clusterId = buffer.getShort();
    final long clusterPosition = buffer.getLong();

    rid = new ORecordId(clusterId, clusterPosition);
  }
}
