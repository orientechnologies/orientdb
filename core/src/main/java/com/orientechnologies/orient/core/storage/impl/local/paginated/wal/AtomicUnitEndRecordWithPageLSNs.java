package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.ORawTriple;
import com.orientechnologies.orient.core.storage.cache.chm.PageKey;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationMetadata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

public class AtomicUnitEndRecordWithPageLSNs extends OAtomicUnitEndRecordV2 {
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
        + (5 * OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE) * pageLSNs.size()
        + OIntegerSerializer.INT_SIZE;
  }

  @Override
  public byte getId() {
    return WALRecordTypes.ATOMIC_UNIT_END_RECORD_WITH_PAGE_LSNS;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OIntegerSerializer.INSTANCE.serializeNative(pageLSNs.size(), content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (final ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber> triple : pageLSNs) {
      final PageKey pageKey = triple.first;

      OLongSerializer.INSTANCE.serializeNative(pageKey.getFileId(), content, offset);
      offset += OLongSerializer.LONG_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(pageKey.getPageIndex(), content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final OLogSequenceNumber startLSN = triple.second;

      OLongSerializer.INSTANCE.serializeNative(startLSN.getSegment(), content, offset);
      offset += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeNative(startLSN.getPosition(), content, offset);
      offset += OLongSerializer.LONG_SIZE;

      final OLogSequenceNumber endLSN = triple.third;

      OLongSerializer.INSTANCE.serializeNative(endLSN.getSegment(), content, offset);
      offset += OLongSerializer.LONG_SIZE;

      OLongSerializer.INSTANCE.serializeNative(endLSN.getPosition(), content, offset);
      offset += OLongSerializer.LONG_SIZE;
    }

    return offset;
  }

  @Override
  public void toStream(ByteBuffer buffer) {
    super.toStream(buffer);

    buffer.putInt(pageLSNs.size());

    for (final ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber> triple : pageLSNs) {
      final PageKey pageKey = triple.first;

      buffer.putLong(pageKey.getFileId());
      buffer.putInt(pageKey.getPageIndex());

      final OLogSequenceNumber startLSN = triple.second;

      buffer.putLong(startLSN.getSegment());
      buffer.putLong(startLSN.getPosition());

      final OLogSequenceNumber endLSN = triple.third;

      buffer.putLong(endLSN.getSegment());
      buffer.putLong(endLSN.getPosition());
    }
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    final int pagesSize = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    pageLSNs = new ArrayList<>(pagesSize);
    for (int i = 0; i < pagesSize; i++) {
      final long fileId = OLongSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OLongSerializer.LONG_SIZE;

      final int pageIndex = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      final PageKey pageKey = new PageKey(fileId, pageIndex);

      final long startSegment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OLongSerializer.LONG_SIZE;

      final long startPosition = OLongSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OLongSerializer.LONG_SIZE;

      final OLogSequenceNumber startLSN = new OLogSequenceNumber(startSegment, startPosition);

      final long endSegment = OLongSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OLongSerializer.LONG_SIZE;

      final long endPosition = OLongSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OLongSerializer.LONG_SIZE;

      final OLogSequenceNumber endLSN = new OLogSequenceNumber(endSegment, endPosition);

      pageLSNs.add(new ORawTriple<>(pageKey, startLSN, endLSN));
    }

    return offset;
  }
}
