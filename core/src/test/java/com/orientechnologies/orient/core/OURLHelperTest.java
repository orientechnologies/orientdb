package com.orientechnologies.orient.core;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import java.io.File;
import org.junit.Test;

/** Created by tglman on 21/02/17. */
public class OURLHelperTest {

  @Test
  public void testSimpleUrl() {
    OURLConnection parsed = OURLHelper.parse("plocal:/path/test/to");
    assertEquals(parsed.getType(), "plocal");
    assertEquals(parsed.getPath(), new File("/path/test").getAbsolutePath());
    assertEquals(parsed.getDbName(), "to");

    parsed = OURLHelper.parse("memory:some");
    assertEquals(parsed.getType(), "memory");
    // assertEquals(parsed.getPath(), "");
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
    assertEquals(parsed.getPath(), new File("/path/test").getAbsolutePath());
    assertEquals(parsed.getDbName(), "to");

    parsed = OURLHelper.parseNew("memory:some");
    assertEquals(parsed.getType(), "embedded");
    assertEquals(parsed.getPath(), "");
    assertEquals(parsed.getDbName(), "some");

    parsed = OURLHelper.parseNew("embedded:/path/test/to");
    assertEquals(parsed.getType(), "embedded");
    assertEquals(parsed.getPath(), new File("/path/test").getAbsolutePath());
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

  @Test()
  public void testRemoteNoDatabase() {
    OURLConnection parsed = OURLHelper.parseNew("remote:localhost");
    assertEquals(parsed.getType(), "remote");
    assertEquals(parsed.getPath(), "localhost");
    assertEquals(parsed.getDbName(), "");

    parsed = OURLHelper.parseNew("remote:localhost:2424");
    assertEquals(parsed.getType(), "remote");
    assertEquals(parsed.getPath(), "localhost:2424");
    assertEquals(parsed.getDbName(), "");

    parsed = OURLHelper.parseNew("remote:localhost:2424/db1");
    assertEquals(parsed.getType(), "remote");
    assertEquals(parsed.getPath(), "localhost:2424");
    assertEquals(parsed.getDbName(), "db1");
  }
}
