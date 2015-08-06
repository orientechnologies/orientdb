package com.orientechnologies.orient.server.network.http;

import java.io.IOException;
import java.net.URLEncoder;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test HTTP "query" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpQueryTest extends BaseHttpDatabaseTest {
  public void queryRootCredentials() throws IOException {
    Assert.assertEquals(get("query/" + getDatabaseName() + "/sql/" + URLEncoder.encode("select from OUSer", "UTF8") + "/10")
        .setUserName("root").setUserPassword("root").getResponse().getStatusLine().getStatusCode(), 200);
  }

  public void queryDatabaseCredentials() throws IOException {
    Assert.assertEquals(get("query/" + getDatabaseName() + "/sql/" + URLEncoder.encode("select from OUSer", "UTF8") + "/10")
        .setUserName("admin").setUserPassword("admin").getResponse().getStatusLine().getStatusCode(), 200);
  }

  @Override
  public String getDatabaseName() {
    return "httpquery";
  }
}
