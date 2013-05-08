package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class SetPageDataRecordTest {
  public void testSerialization() {
    OSetPageDataRecord serializedSetPageDataRecord = new OSetPageDataRecord(12, "test");

    byte[] dataOne = new byte[] { 1, 2, 3 };
    byte[] dataTwo = new byte[] { 4, 5, 6 };
    serializedSetPageDataRecord.addDiff(34, dataOne);
    serializedSetPageDataRecord.addDiff(43, dataTwo);

    byte[] content = new byte[serializedSetPageDataRecord.serializedSize() + 1];

    int toStreamOffset = serializedSetPageDataRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OSetPageDataRecord restoredSetPageDataRecord = new OSetPageDataRecord();
    int fromStreamOffset = restoredSetPageDataRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredSetPageDataRecord.getFileName(), "test");
    Assert.assertEquals(restoredSetPageDataRecord.getPageIndex(), 12);

    Assert.assertEquals(restoredSetPageDataRecord.getDiffs(), serializedSetPageDataRecord.getDiffs());
  }
}
