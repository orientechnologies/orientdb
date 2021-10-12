package com.orientechnologies.common.comparator;

import java.util.Comparator;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @author Daniel Ritter
 * @since 11.07.12
 */
public class UnsafeComparatorTest {
  private OUnsafeByteArrayComparator comparator = OUnsafeByteArrayComparator.INSTANCE;

  @Test
  public void testOneByteArray() {
    final byte[] keyOne = new byte[] {1};
    final byte[] keyTwo = new byte[] {2};

    assertCompareTwoKeys(comparator, keyOne, keyTwo);
  }

  @Test
  public void testOneLongArray() {
    final byte[] keyOne = new byte[] {0, 1, 0, 0, 0, 0, 0, 0};
    final byte[] keyTwo = new byte[] {1, 0, 0, 0, 0, 0, 0, 0};

    assertCompareTwoKeys(comparator, keyOne, keyTwo);
  }

  @Test
  public void testOneLongArrayAndByte() {
    final byte[] keyOne = new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 0};
    final byte[] keyTwo = new byte[] {1, 1, 0, 0, 0, 0, 0, 0, 1};

    assertCompareTwoKeys(comparator, keyOne, keyTwo);
  }

  @Test
  public void testOneArraySmallerThanOther() {
    final byte[] keyOne =
        new byte[] {
          1, 1, 0, 0, 1, 0,
        };
    final byte[] keyTwo = new byte[] {1, 1, 0, 0, 1, 0, 0, 0, 1};

    assertCompareTwoKeys(comparator, keyOne, keyTwo);
  }

  private void assertCompareTwoKeys(
      final Comparator<byte[]> comparator, byte[] keyOne, byte[] keyTwo) {
    Assert.assertTrue(comparator.compare(keyOne, keyTwo) < 0);
    Assert.assertTrue(comparator.compare(keyTwo, keyOne) > 0);
    //noinspection EqualsWithItself
    Assert.assertEquals(0, comparator.compare(keyTwo, keyTwo));
  }
}
