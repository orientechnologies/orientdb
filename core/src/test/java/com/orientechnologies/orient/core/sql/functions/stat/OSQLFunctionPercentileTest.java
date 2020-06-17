package com.orientechnologies.orient.core.sql.functions.stat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class OSQLFunctionPercentileTest {

  private OSQLFunctionPercentile percentile;

  @Before
  public void beforeMethod() {
    percentile =
        new OSQLFunctionPercentile() {
          @Override
          protected boolean returnDistributedResult() {
            return false;
          }
        };
  }

  @Test
  public void testEmpty() {
    Object result = percentile.getResult();
    assertNull(result);
  }

  @Test
  public void testSingleValueLower() {
    percentile.execute(null, null, null, new Object[] {10, .25}, null);
    assertEquals(10, percentile.getResult());
  }

  @Test
  public void testSingleValueUpper() {
    percentile.execute(null, null, null, new Object[] {10, .75}, null);
    assertEquals(10, percentile.getResult());
  }

  @Test
  public void test50thPercentileOdd() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .5}, null);
    }

    Object result = percentile.getResult();
    assertEquals(3.0, result);
  }

  @Test
  public void test50thPercentileOddWithNulls() {
    Integer[] scores = {null, 1, 2, null, 3, 4, null, 5};

    for (Integer s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .5}, null);
    }

    Object result = percentile.getResult();
    assertEquals(3.0, result);
  }

  @Test
  public void test50thPercentileEven() {
    int[] scores = {1, 2, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .5}, null);
    }

    Object result = percentile.getResult();
    assertEquals(3.0, result);
  }

  @Test
  public void testFirstQuartile() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .25}, null);
    }

    Object result = percentile.getResult();
    assertEquals(1.5, result);
  }

  @Test
  public void testThirdQuartile() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .75}, null);
    }

    Object result = percentile.getResult();
    assertEquals(4.5, result);
  }

  @Test
  public void testMultiQuartile() {
    int[] scores = {1, 2, 3, 4, 5};

    for (int s : scores) {
      percentile.execute(null, null, null, new Object[] {s, .25, .75}, null);
    }

    List<Number> result = (List<Number>) percentile.getResult();
    assertEquals(1.5, result.get(0).doubleValue(), 0);
    assertEquals(4.5, result.get(1).doubleValue(), 0);
  }
}
