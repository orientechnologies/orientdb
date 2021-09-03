package com.orientechnologies.orient.core.sql.functions.stat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class OSQLFunctionModeTest {

  private OSQLFunctionMode mode;

  @Before
  public void setup() {
    mode =
        new OSQLFunctionMode() {
          @Override
          protected boolean returnDistributedResult() {
            return false;
          }
        };
  }

  @Test
  public void testEmpty() {
    Object result = mode.getResult();
    assertNull(result);
  }

  @Test
  public void testSingleMode() {
    int[] scores = {1, 2, 3, 3, 3, 2};

    for (int s : scores) {
      mode.execute(null, null, null, new Object[] {s}, null);
    }

    Object result = mode.getResult();
    assertEquals(3, (int) ((List<Integer>) result).get(0));
  }

  @Test
  public void testMultiMode() {
    int[] scores = {1, 2, 3, 3, 3, 2, 2};

    for (int s : scores) {
      mode.execute(null, null, null, new Object[] {s}, null);
    }

    Object result = mode.getResult();
    List<Integer> modes = (List<Integer>) result;
    assertEquals(2, modes.size());
    assertTrue(modes.contains(2));
    assertTrue(modes.contains(3));
  }

  @Test
  public void testMultiValue() {
    List[] scores = new List[2];
    scores[0] = Arrays.asList(new Integer[] {1, 2, null, 3, 4});
    scores[1] = Arrays.asList(new Integer[] {1, 1, 1, 2, null});

    for (List s : scores) {
      mode.execute(null, null, null, new Object[] {s}, null);
    }

    Object result = mode.getResult();
    assertEquals(1, (int) ((List<Integer>) result).get(0));
  }
}
