package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
@Test
public class StartAtomicPageUpdateTest {
  public void testSerialization() {
    OStartAtomicPageUpdateRecord startAtomicPageUpdateOriginal = new OStartAtomicPageUpdateRecord(142, "test");
    int serializedSize = startAtomicPageUpdateOriginal.serializedSize();

    byte[] content = new byte[serializedSize + 1];
    int toStreamOffset = startAtomicPageUpdateOriginal.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OStartAtomicPageUpdateRecord startAtomicPageUpdateRestored = new OStartAtomicPageUpdateRecord();
    int fromStreamOffset = startAtomicPageUpdateRestored.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(startAtomicPageUpdateRestored.getFileName(), "test");
    Assert.assertEquals(startAtomicPageUpdateRestored.getPageIndex(), 142);
  }
}
