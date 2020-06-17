package com.orientechnologies.orient.core.sql.functions.math;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class OSQLFunctionIntervalTest {

  private OSQLFunctionInterval function;

  @Before
  public void setup() {
    function = new OSQLFunctionInterval();
  }

  @Test
  public void testNull() {
    // should throw exception - minimum 2 arguments
    doTest(-1, (Object[]) null);
  }

  @Test
  public void testSingleArgument() {
    // should throw exception - minimum 2
    doTest(-1, 53);
  }

  @Test
  public void testMultiple() {
    doTest(3, 43, 35, 5, 15, 50);
    doTest(-1, 54, 25, 35, 45);
    doTest(-1, null, 5, 50);
    doTest(-1, 6, 6);
    doTest(0, 58, 60, 30, 65);
    doTest(1, 103, 54, 106, 98, 119);
  }

  private void doTest(int expectedResult, Object... params) {
    final Object result = function.execute(null, null, null, params, null);
    assertEquals(expectedResult, result);
  }
}
