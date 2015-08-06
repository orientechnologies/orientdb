package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URLEncoder;

/**
 * Test HTTP authentication command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpAuthenticationTest extends BaseHttpDatabaseTest {
  public void testChangeOfUserOnSameConnectionIsAllowed() throws IOException {
    Assert.assertEquals(get("query/" + getDatabaseName() + "/sql/" + URLEncoder.encode("select from OUSer", "UTF8") + "/10")
        .setUserName("root").setUserPassword("root").getResponse().getStatusLine().getStatusCode(), 200);

    Assert.assertEquals(get("query/" + getDatabaseName() + "/sql/" + URLEncoder.encode("select from OUSer", "UTF8") + "/10")
        .setUserName("admin").setUserPassword("admin").getResponse().getStatusLine().getStatusCode(), 200);
  }

  @Override
  public String getDatabaseName() {
    return "httpauth";
  }
}
