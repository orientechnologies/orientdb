package com.orientechnologies.common.comparator;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 11.07.12
 */
@Test(enabled = false)
public class UnsafeComparatorTest {
  public void testOneByteArray() {
    final byte[] keyOne = new byte[] { 1 };
    final byte[] keyTwo = new byte[] { 2 };

    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyOne, keyTwo) < 0);
    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyTwo, keyOne) > 0);
    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyTwo, keyTwo) == 0);
  }

  public void testOneLongArray() {
    final byte[] keyOne = new byte[] { 0, 1, 0, 0, 0, 0, 0, 0 };
    final byte[] keyTwo = new byte[] { 1, 0, 0, 0, 0, 0, 0, 0 };

    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyOne, keyTwo) < 0);
    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyTwo, keyOne) > 0);
    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyTwo, keyTwo) == 0);
  }

  public void testOneLongArrayAndByte() {
    final byte[] keyOne = new byte[] { 1, 1, 0, 0, 0, 0, 0, 0, 0 };
    final byte[] keyTwo = new byte[] { 1, 1, 0, 0, 0, 0, 0, 0, 1 };

    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyOne, keyTwo) < 0);
    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyTwo, keyOne) > 0);
    Assert.assertTrue(OUnsafeByteArrayComparator.INSTANCE.compare(keyTwo, keyTwo) == 0);
  }
}
