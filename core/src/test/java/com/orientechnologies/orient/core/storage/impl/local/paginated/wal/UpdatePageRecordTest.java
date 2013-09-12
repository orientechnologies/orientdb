package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class UpdatePageRecordTest {
  public void testSerializationPrevLSNIsNotNull() {
    OPageChanges pageChanges = new OPageChanges();

    OLogSequenceNumber prevLsn = new OLogSequenceNumber(12, 124);
    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, pageChanges, prevLsn);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
  }

  public void testSerializationPrevLSNIsNull() {
    OPageChanges pageChanges = new OPageChanges();

    OLogSequenceNumber prevLsn = new OLogSequenceNumber(12, 124);
    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, pageChanges, prevLsn);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
  }

}
