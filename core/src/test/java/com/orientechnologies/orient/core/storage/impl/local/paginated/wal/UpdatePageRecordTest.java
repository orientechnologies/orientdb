package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OBinaryDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OIntDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OLongDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OUpdatePageRecord;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class UpdatePageRecordTest {
  public void testSerializationPrevLSNIsNotNull() {
    OLogSequenceNumber lsn = new OLogSequenceNumber(5, 100);
    List<OPageDiff<?>> diffs = new ArrayList<OPageDiff<?>>();

    diffs.add(new OBinaryDiff(new byte[] { 7, 4, 8 }, 13));
    diffs.add(new OIntDiff(19, 42));
    diffs.add(new OLongDiff(37L, 213));

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
    List<OPageDiff<?>> diffs = new ArrayList<OPageDiff<?>>();

    diffs.add(new OBinaryDiff(new byte[] { 7, 4, 8 }, 13));
    diffs.add(new OIntDiff(19, 42));
    diffs.add(new OLongDiff(37L, 213));

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
