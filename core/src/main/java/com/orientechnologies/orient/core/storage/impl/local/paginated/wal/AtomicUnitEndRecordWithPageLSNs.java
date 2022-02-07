package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.ORawTriple;
import com.orientechnologies.orient.core.storage.cache.chm.PageKey;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

public class AtomicUnitEndRecordWithPageLSNs extends OAtomicUnitEndRecord {
  private ArrayList<ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber>> pageLSNs;

  public AtomicUnitEndRecordWithPageLSNs() {}

  public AtomicUnitEndRecordWithPageLSNs(
      final long operationUnitId,
      final boolean rollback,
      final Map<String, OAtomicOperationMetadata<?>> atomicOperationMetadataMap,
      final ArrayList<ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber>> pageLSNs) {
    super(operationUnitId, rollback, atomicOperationMetadataMap);
    this.pageLSNs = pageLSNs;
  }

  public ArrayList<ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber>> getPageLSNs() {
    return pageLSNs;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize()
        + (3 * OLongSerializer.LONG_SIZE + 3 * OIntegerSerializer.INT_SIZE) * pageLSNs.size()
        + OIntegerSerializer.INT_SIZE;
  }

  @Override
  protected void serializeToByteBuffer(ByteBuffer buffer) {
    super.serializeToByteBuffer(buffer);

    buffer.putInt(pageLSNs.size());
    for (final ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber> triple : pageLSNs) {
      final PageKey pageKey = triple.first;

      buffer.putLong(pageKey.getFileId());
      buffer.putInt(pageKey.getPageIndex());

      final OLogSequenceNumber startLSN = triple.second;
      buffer.putLong(startLSN.getSegment());
      buffer.putInt(startLSN.getPosition());

      final OLogSequenceNumber endLSN = triple.third;
      buffer.putLong(endLSN.getSegment());
      buffer.putInt(endLSN.getPosition());
    }
  }

  @Override
  protected void deserializeFromByteBuffer(ByteBuffer buffer) {
    super.deserializeFromByteBuffer(buffer);
    final int pagesSize = buffer.getInt();
    pageLSNs = new ArrayList<>(pagesSize);

    for (int i = 0; i < pagesSize; i++) {
      final long fileId = buffer.getLong();
      final int pageIndex = buffer.getInt();

      final PageKey pageKey = new PageKey(fileId, pageIndex);

      final long startLsnSegment = buffer.getLong();
      final int startLsnPosition = buffer.getInt();

      final OLogSequenceNumber startLsn = new OLogSequenceNumber(startLsnSegment, startLsnPosition);

      final long endLsnSegment = buffer.getLong();
      final int endLsnPosition = buffer.getInt();

      final OLogSequenceNumber endLSN = new OLogSequenceNumber(endLsnSegment, endLsnPosition);

      final ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber> pageLsn =
          new ORawTriple<>(pageKey, startLsn, endLSN);

      pageLSNs.add(pageLsn);
    }
  }

  @Override
  public int getId() {
    return WALRecordTypes.ATOMIC_UNIT_END_RECORD_WITH_PAGE_LSNS;
  }
}
