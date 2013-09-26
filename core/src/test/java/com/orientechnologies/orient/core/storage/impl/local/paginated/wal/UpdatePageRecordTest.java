package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OBinaryFullPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OBinaryPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OIntFullPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OIntPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OLongFullPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OLongPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OPageDiff;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.updatePageRecord.OUpdatePageRecord;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class UpdatePageRecordTest {
  public void testSerializationPrevLSNIsNotNull() {
    List<OPageDiff<?>> diffs = new ArrayList<OPageDiff<?>>();

    diffs.add(new OBinaryPageDiff(new byte[] { 7, 4, 8 }, 13));
    diffs.add(new OIntPageDiff(19, 42));
    diffs.add(new OLongPageDiff(37L, 213));

    diffs.add(new OBinaryFullPageDiff(new byte[] { 7, 4, 8 }, 23, new byte[] { 12, 10, 8 }));
    diffs.add(new OIntFullPageDiff(19, 46, 70));
    diffs.add(new OLongFullPageDiff(37L, 213, 45L));

    OLogSequenceNumber prevLsn = new OLogSequenceNumber(12, 124);
    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, diffs, prevLsn);

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

    diffs.add(new OBinaryPageDiff(new byte[] { 7, 4, 8 }, 13));
    diffs.add(new OIntPageDiff(19, 42));
    diffs.add(new OLongPageDiff(37L, 213));

    diffs.add(new OBinaryFullPageDiff(new byte[] { 7, 4, 8 }, 23, new byte[] { 12, 10, 8 }));
    diffs.add(new OIntFullPageDiff(19, 46, 70));
    diffs.add(new OLongFullPageDiff(37L, 213, 45L));

    OLogSequenceNumber prevLsn = new OLogSequenceNumber(12, 124);
    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, diffs, prevLsn);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
  }

}
