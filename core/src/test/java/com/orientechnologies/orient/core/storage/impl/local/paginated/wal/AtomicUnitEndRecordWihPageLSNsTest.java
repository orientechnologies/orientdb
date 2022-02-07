package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.util.ORawTriple;
import com.orientechnologies.orient.core.storage.cache.chm.PageKey;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class AtomicUnitEndRecordWihPageLSNsTest {
  @Test
  public void pageLsnSerializationTest() {
    final ArrayList<ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber>> pageLSNs =
        new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      final PageKey pageKey = new PageKey(i, 42 + i);
      final OLogSequenceNumber startLSN = new OLogSequenceNumber(12 + i, 35 + i);
      final OLogSequenceNumber endLSN = new OLogSequenceNumber(78 + i, 90 + i);

      final ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber> lsn =
          new ORawTriple<>(pageKey, startLSN, endLSN);
      pageLSNs.add(lsn);
    }

    AtomicUnitEndRecordWithPageLSNs record =
        new AtomicUnitEndRecordWithPageLSNs(1, false, null, pageLSNs);
    final int serializedSize = record.serializedSize();

    final ByteBuffer buffer =
        ByteBuffer.allocate(serializedSize + 1).order(ByteOrder.nativeOrder());
    buffer.position(1);
    record.toStream(buffer);
    Assert.assertEquals(serializedSize + 1, buffer.position());

    AtomicUnitEndRecordWithPageLSNs newRecord = new AtomicUnitEndRecordWithPageLSNs();
    final int finalOffset = newRecord.fromStream(buffer.array(), 1);
    Assert.assertEquals(serializedSize + 1, finalOffset);

    final ArrayList<ORawTriple<PageKey, OLogSequenceNumber, OLogSequenceNumber>> newPageLSNs =
        newRecord.getPageLSNs();
    Assert.assertEquals(pageLSNs, newPageLSNs);
  }
}
