package com.orientechnologies.orient.core.sql.method.misc;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class OSQLMethodValuesTest {

  private OSQLMethodValues function;

  @Before
  public void setup() {
    function = new OSQLMethodValues();
  }

  @Test
  public void testWithOResult() {

    OResultInternal resultInternal = new OResultInternal();
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    Object result = function.execute(null, null, null, resultInternal, null);
    assertEquals(Arrays.asList("Foo", "Bar"), result);
  }
}
