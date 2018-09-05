package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 29.04.13
 */
public class UpdatePageRecordTest {
  @Test
  public void testSerializationPrevLSNIsNotNull() {
    OWALChanges changesTree = new OWALPageChangesPortion();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changesTree,
        new OLogSequenceNumber(12, 34));

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
    Assert.assertEquals(new OLogSequenceNumber(12, 34), restoredUpdatePageRecord.getPrevLsn());
  }

  @Test
  public void testSerializationPrevLSNIsNull() {
    OWALChanges changesTree = new OWALPageChangesPortion();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changesTree,
        new OLogSequenceNumber(15, 72));

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
    Assert.assertEquals(new OLogSequenceNumber(15, 72), restoredUpdatePageRecord.getPrevLsn());
  }

}
