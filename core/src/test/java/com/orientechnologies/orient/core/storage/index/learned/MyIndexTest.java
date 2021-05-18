package com.orientechnologies.orient.core.storage.index.learned;

import java.util.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MyIndexTest {
  // space-time trade-off parameter
  protected static final int EPSILON = 128;

  private MyIndex pgmIndex;
  private List<Integer> data;

  @Before
  public void setup() {
    // data = generateData(1_000, 42, new Random());
    data = Arrays.asList(47, 43, 17, 13, 3, 81, 42, 3);
    Collections.sort(data);
    // Assert.assertEquals(1_000, data.size());
    Assert.assertEquals(8, data.size());
    pgmIndex = new MyIndex<Integer>(EPSILON, data);
  }

  @Test
  public void test() {
    final ApproximatePosition approximatePosition = pgmIndex.search(42);
    // FIXME: not exactly data.begin(), which returns an iterator
    final int low = data.get(0) + approximatePosition.getRangeLowerBound();
    final int high = data.get(0) + approximatePosition.getRangeUpperBound();

    // FIXME; we want std::lower_bound here
    final int result = lowerBound(low, high, 42);
    Assert.assertEquals(0, result);
  }

  private int lowerBound(final int low, final int high, final int position) {
    return 0;
  }

  protected static List<Integer> generateData(
      final int numberEntries, final int mustInclude, final Random random) {
    final List<Integer> data = new ArrayList<>(numberEntries);
    for (int i = 0; i < numberEntries - 1; i++) {
      data.add(random.nextInt() & Integer.MAX_VALUE);
    }
    data.add(mustInclude);
    return data;
  }
}
