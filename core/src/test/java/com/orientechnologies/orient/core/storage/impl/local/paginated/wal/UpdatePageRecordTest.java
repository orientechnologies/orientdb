package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class UpdatePageRecordTest {
  public void testSerializationPrevLSNIsNotNull() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(5, 100);
    List<OUpdatePageRecord.Diff<?>> diffs = new ArrayList<OUpdatePageRecord.Diff<?>>();

    diffs.add(new OUpdatePageRecord.BinaryDiff(new byte[] { 1, 2, 6 }, new byte[] { 5, 1, 2, 8 }, 123));
    diffs.add(new OUpdatePageRecord.IntDiff(10, 23, 56));
    diffs.add(new OUpdatePageRecord.LongDiff(34L, 56L, 23));

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, lsn, diffs);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
  }

  public void testSerializationPrevLSNIsNull() {
    List<OUpdatePageRecord.Diff<?>> diffs = new ArrayList<OUpdatePageRecord.Diff<?>>();

    diffs.add(new OUpdatePageRecord.BinaryDiff(new byte[] { 1, 2, 6 }, new byte[] { 5, 1, 2, 8 }, 123));
    diffs.add(new OUpdatePageRecord.IntDiff(10, 23, 56));
    diffs.add(new OUpdatePageRecord.LongDiff(34L, 56L, 23));

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, null, diffs);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
  }

}
