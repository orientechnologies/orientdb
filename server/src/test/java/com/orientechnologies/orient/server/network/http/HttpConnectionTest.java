package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests HTTP "connect" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpConnectionTest extends BaseHttpDatabaseTest {
  @Test
  public void testConnect() throws Exception {
    Assert.assertEquals(get("connect/" + getDatabaseName()).getResponse().getStatusLine().getStatusCode(), 204);
  }

  @Override
  public String getDatabaseName() {
    return "httpconnection";
  }
}
