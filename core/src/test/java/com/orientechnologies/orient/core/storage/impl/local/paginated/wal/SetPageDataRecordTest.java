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
    OSetPageDataRecord serializedSetPageDataRecord = new OSetPageDataRecord(new byte[] { 0, 1, 2, 3, 4, 5 }, 10, 12, "test");

    byte[] content = new byte[serializedSetPageDataRecord.serializedSize() + 1];

    int toStreamOffset = serializedSetPageDataRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OSetPageDataRecord restoredSetPageDataRecord = new OSetPageDataRecord();
    int fromStreamOffset = restoredSetPageDataRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredSetPageDataRecord.getFileName(), "test");
    Assert.assertEquals(restoredSetPageDataRecord.getPageIndex(), 12);
    Assert.assertEquals(restoredSetPageDataRecord.getData(), new byte[] { 0, 1, 2, 3, 4, 5 });
    Assert.assertEquals(restoredSetPageDataRecord.getPageOffset(), 10);
  }
}
