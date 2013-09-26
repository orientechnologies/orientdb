package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 17.06.13
 */
@Test
public class FreePageChangeRecordTest {
  public void testSerialization() {
    OOperationUnitId unitId = OOperationUnitId.generateId();

    OFreePageChangeRecord serializedFreePageChangeRecord = new OFreePageChangeRecord(unitId, 1, 10, 123, 345);

    byte[] content = new byte[serializedFreePageChangeRecord.serializedSize() + 1];

    int toStreamOffset = serializedFreePageChangeRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OFreePageChangeRecord restoredFreePageChangeRecord = new OFreePageChangeRecord();
    int fromStreamOffset = restoredFreePageChangeRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredFreePageChangeRecord, serializedFreePageChangeRecord);
  }
}
