package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.localhashtable.v2.metadatapage;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WALRecordTypes;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.po.PageOperationRecord;
import com.orientechnologies.orient.core.storage.index.hashindex.local.v2.HashIndexMetadataPageV2;

import java.nio.ByteBuffer;

public final class LocalHashTableV2MetadataPageSetRecordsCountPO extends PageOperationRecord {
  private long recordsCount;
  private long pastRecordsCount;

  public LocalHashTableV2MetadataPageSetRecordsCountPO() {
  }

  public LocalHashTableV2MetadataPageSetRecordsCountPO(long recordsCount, long pastRecordsCount) {
    this.recordsCount = recordsCount;
    this.pastRecordsCount = pastRecordsCount;
  }

  public long getRecordsCount() {
    return recordsCount;
  }

  public long getPastRecordsCount() {
    return pastRecordsCount;
  }

  @Override
  public void redo(OCacheEntry cacheEntry) {
    final HashIndexMetadataPageV2 page = new HashIndexMetadataPageV2(cacheEntry);
    page.setRecordsCount(recordsCount);
  }

  @Override
  public void undo(OCacheEntry cacheEntry) {
    final HashIndexMetadataPageV2 page = new HashIndexMetadataPageV2(cacheEntry);
    page.setRecordsCount(pastRecordsCount);
  }

  @Override
  public int getId() {
    return WALRecordTypes.LOCAL_HASH_TABLE_V2_METADATA_PAGE_SET_RECORDS_COUNT_PO;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + 2 * OLongSerializer.LONG_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putLong(recordsCount);
    buffer.putLong(pastRecordsCount);
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);

    recordsCount = buffer.getLong();
    pastRecordsCount = buffer.getLong();
  }
}
