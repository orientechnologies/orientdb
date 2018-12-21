package com.orientechnologies.orient.core.sql.method.misc;

import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashSet;

import static org.junit.Assert.assertEquals;

public class OSQLMethodKeysTest {

  private OSQLMethodKeys function;

  @Before
  public void setup() {
    function = new OSQLMethodKeys();
  }

  @Test
  public void testWithOResult() {

    OResultInternal resultInternal = new OResultInternal();
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    Object result = function.execute(null, null, null, resultInternal, null);
    assertEquals(new LinkedHashSet(Arrays.asList("name", "surname")), result);
  }

}
