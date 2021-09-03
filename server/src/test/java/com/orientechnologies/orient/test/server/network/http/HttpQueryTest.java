package com.orientechnologies.orient.test.server.network.http;

import java.io.IOException;
import java.net.URLEncoder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test HTTP "query" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpQueryTest extends BaseHttpDatabaseTest {
  @Test
  public void queryRootCredentials() throws IOException {
    Assert.assertEquals(
        get("query/"
                + getDatabaseName()
                + "/sql/"
                + URLEncoder.encode("select from OUSer", "UTF8")
                + "/10")
            .setUserName("root")
            .setUserPassword("root")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Test
  public void queryDatabaseCredentials() throws IOException {
    Assert.assertEquals(
        get("query/"
                + getDatabaseName()
                + "/sql/"
                + URLEncoder.encode("select from OUSer", "UTF8")
                + "/10")
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Override
  public String getDatabaseName() {
    return "httpquery";
  }
}
