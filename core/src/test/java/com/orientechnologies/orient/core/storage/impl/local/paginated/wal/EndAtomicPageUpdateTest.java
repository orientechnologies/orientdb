package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 26.04.13
 */
@Test
public class EndAtomicPageUpdateTest {
  public void testSerialization() {
    OEndAtomicPageUpdateRecord endAtomicPageUpdateOriginal = new OEndAtomicPageUpdateRecord(134, "test");
    int serializedSize = endAtomicPageUpdateOriginal.serializedSize();

    byte[] content = new byte[serializedSize + 1];
    int toStreamOffset = endAtomicPageUpdateOriginal.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OEndAtomicPageUpdateRecord endAtomicPageUpdateRestored = new OEndAtomicPageUpdateRecord();
    int fromStreamOffset = endAtomicPageUpdateRestored.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(endAtomicPageUpdateRestored.getFileName(), "test");
    Assert.assertEquals(endAtomicPageUpdateRestored.getPageIndex(), 134);
  }
}
