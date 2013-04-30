package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class ShiftPageDataRecordTest {
  public void testSerialization() {
    OShiftPageDataRecord serializedShiftPageDataRecord = new OShiftPageDataRecord(20, 23, 14, "test", 12);
    byte[] content = new byte[serializedShiftPageDataRecord.serializedSize() + 1];
    int toStreamOffset = serializedShiftPageDataRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OShiftPageDataRecord restoredShiftPageDataRecord = new OShiftPageDataRecord();
    int fromStreamOffset = restoredShiftPageDataRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredShiftPageDataRecord.getPageIndex(), 12);
    Assert.assertEquals(restoredShiftPageDataRecord.getFrom(), 20);
    Assert.assertEquals(restoredShiftPageDataRecord.getTo(), 23);
    Assert.assertEquals(restoredShiftPageDataRecord.getLen(), 14);
    Assert.assertEquals(restoredShiftPageDataRecord.getFileName(), "test");
  }
}
