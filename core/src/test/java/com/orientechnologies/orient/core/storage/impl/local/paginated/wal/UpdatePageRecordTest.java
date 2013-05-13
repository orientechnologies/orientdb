package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class UpdatePageRecordTest {
  public void testSerialization() {
    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, "test");

    byte[] dataOne = new byte[] { 1, 2, 3 };
    byte[] dataTwo = new byte[] { 4, 5, 6 };
    serializedUpdatePageRecord.addDiff(34, dataOne);
    serializedUpdatePageRecord.addDiff(43, dataTwo);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord.getFileName(), "test");
    Assert.assertEquals(restoredUpdatePageRecord.getPageIndex(), 12);

    Assert.assertEquals(restoredUpdatePageRecord.getDiffs(), serializedUpdatePageRecord.getDiffs());
  }
}
