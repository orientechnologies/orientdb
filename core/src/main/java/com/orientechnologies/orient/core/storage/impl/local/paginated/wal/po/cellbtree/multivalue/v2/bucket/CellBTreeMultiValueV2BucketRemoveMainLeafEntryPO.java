package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.cellbtree.multivalue.v2.bucket;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2.CellBTreeMultiValueV2Bucket;

import java.nio.ByteBuffer;

public final class CellBTreeMultiValueV2BucketRemoveMainLeafEntryPO extends PageOperationRecord {
  private int    index;
  private byte[] key;
  private ORID   value;
  private long   mId;

  public CellBTreeMultiValueV2BucketRemoveMainLeafEntryPO() {
  }

  public CellBTreeMultiValueV2BucketRemoveMainLeafEntryPO(int index, byte[] key, ORID value, long mId) {
    this.index = index;
    this.key = key;
    this.value = value;
    this.mId = mId;
  }

  public int getIndex() {
    return index;
  }

  public byte[] getKey() {
    return key;
  }

  public ORID getValue() {
    return value;
  }

  public long getmId() {
    return mId;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2Bucket bucket = new CellBTreeMultiValueV2Bucket(cacheEntry);
    bucket.removeMainLeafEntry(index, key.length);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final CellBTreeMultiValueV2Bucket bucket = new CellBTreeMultiValueV2Bucket(cacheEntry);
    final boolean result = bucket.createMainLeafEntry(index, key, value, mId);
    if (!result) {
      throw new IllegalStateException("Can not undo main leaf entry creation");
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.CELL_BTREE_BUCKET_MULTI_VALUE_V2_REMOVE_MAIN_LEAF_ENTRY_PO;
  }

  @Override
  public int serializedSize() {
    int size = 2 * OIntegerSerializer.INT_SIZE + key.length + OLongSerializer.LONG_SIZE + OByteSerializer.BYTE_SIZE;
    if (value != null) {
      size += OLongSerializer.LONG_SIZE + OShortSerializer.SHORT_SIZE;
    }

    return super.serializedSize() + size;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(index);

    buffer.putInt(key.length);
    buffer.put(key);

    buffer.putLong(mId);

    if (value == null) {
      buffer.put((byte) 0);
    } else {
      buffer.put((byte) 1);

      buffer.putShort((short) value.getClusterId());
      buffer.putLong(value.getClusterPosition());
    }
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    index = buffer.getInt();

    final int len = buffer.getInt();
    key = new byte[len];
    buffer.get(key);

    mId = buffer.getLong();

    if (buffer.get() > 0) {
      final int clusterId = buffer.getShort();
      final long clusterPosition = buffer.getLong();

      value = new ORecordId(clusterId, clusterPosition);
    }
  }
}
