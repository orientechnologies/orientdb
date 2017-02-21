package com.orientechnologies.orient.core;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 21/02/17.
 */
public class TestOURLHelper {

  @Test
  public void testSimpleUrl() {
    OURLConnection parsed = OURLHelper.parse("plocal:/path/test/to");
    assertEquals(parsed.getType(), "plocal");
    assertEquals(parsed.getPath(), "/path/test");
    assertEquals(parsed.getDbName(), "to");

    parsed = OURLHelper.parse("memory:some");
    assertEquals(parsed.getType(), "memory");
    //assertEquals(parsed.getPath(), "");
    assertEquals(parsed.getDbName(), "some");

    parsed = OURLHelper.parse("remote:localhost/to");
    assertEquals(parsed.getType(), "remote");
    assertEquals(parsed.getPath(), "localhost");
    assertEquals(parsed.getDbName(), "to");
  }

  @Test
  public void testSimpleNewUrl() {
    OURLConnection parsed = OURLHelper.parseNew("plocal:/path/test/to");
    assertEquals(parsed.getType(), "embedded");
    assertEquals(parsed.getPath(), "/path/test");
    assertEquals(parsed.getDbName(), "to");

    parsed = OURLHelper.parseNew("memory:some");
    assertEquals(parsed.getType(), "embedded");
    assertEquals(parsed.getPath(), "");
    assertEquals(parsed.getDbName(), "some");

    parsed = OURLHelper.parseNew("embedded:/path/test/to");
    assertEquals(parsed.getType(), "embedded");
    assertEquals(parsed.getPath(), "/path/test");
    assertEquals(parsed.getDbName(), "to");

    parsed = OURLHelper.parseNew("remote:localhost/to");
    assertEquals(parsed.getType(), "remote");
    assertEquals(parsed.getPath(), "localhost");
    assertEquals(parsed.getDbName(), "to");

  }

  @Test(expected = OConfigurationException.class)
  public void testWrongPrefix() {
    OURLHelper.parseNew("embd:/path/test/to");
  }

  @Test(expected = OConfigurationException.class)
  public void testNoPrefix() {
    OURLHelper.parseNew("/embd/path/test/to");
  }

}
