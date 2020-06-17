package com.orientechnologies.orient.core.sql.method.misc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.sql.functions.text.OSQLMethodSubString;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the "asList()" method implemented by the OSQLMethodAsList class. Note that the only input
 * to the execute() method from the OSQLMethod interface that is used is the ioResult argument (the
 * 4th argument).
 *
 * @author Michael MacFadden
 */
public class OSQLMethodSubStringTest {

  private OSQLMethodSubString function;

  @Before
  public void setup() {
    function = new OSQLMethodSubString();
  }

  @Test
  public void testRange() {

    Object result = function.execute("foobar", null, null, null, new Object[] {1, 3});
    assertEquals(result, "foobar".substring(1, 3));

    result = function.execute("foobar", null, null, null, new Object[] {0, 0});
    assertEquals(result, "foobar".substring(0, 0));

    result = function.execute("foobar", null, null, null, new Object[] {0, 1000});
    assertEquals(result, "foobar");

    result = function.execute("foobar", null, null, null, new Object[] {0, -1});
    assertEquals(result, "");

    result = function.execute("foobar", null, null, null, new Object[] {6, 6});
    assertEquals(result, "foobar".substring(6, 6));

    result = function.execute("foobar", null, null, null, new Object[] {1, 9});
    assertEquals(result, "foobar".substring(1, 6));

    result = function.execute("foobar", null, null, null, new Object[] {-7, 4});
    assertEquals(result, "foobar".substring(0, 4));
  }

  @Test
  public void testFrom() {
    Object result = function.execute("foobar", null, null, null, new Object[] {1});
    assertEquals(result, "foobar".substring(1));

    result = function.execute("foobar", null, null, null, new Object[] {0});
    assertEquals(result, "foobar".substring(0));

    result = function.execute("foobar", null, null, null, new Object[] {6});
    assertEquals(result, "foobar".substring(6));

    result = function.execute("foobar", null, null, null, new Object[] {12});
    assertEquals(result, "");

    result = function.execute("foobar", null, null, null, new Object[] {-7});
    assertEquals(result, "foobar".substring(0));
  }

  @Test
  public void testNull() {

    Object result = function.execute(null, null, null, null, null);
    assertNull(result);
  }
}
