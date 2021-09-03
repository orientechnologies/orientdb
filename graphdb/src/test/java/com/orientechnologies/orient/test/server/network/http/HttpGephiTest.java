package com.orientechnologies.orient.test.server.network.http;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test HTTP "gephi" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpGephiTest extends BaseHttpDatabaseTest {
  @Test
  public void commandRootCredentials() throws IOException {
    Assert.assertEquals(
        get("gephi/" + getDatabaseName() + "/sql/select%20from%20V")
            .setUserName("root")
            .setUserPassword("root")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Test
  public void commandDatabaseCredentials() throws IOException {
    Assert.assertEquals(
        get("gephi/" + getDatabaseName() + "/sql/select%20from%20V")
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Before
  @Override
  public void createDatabase() throws Exception {
    super.createDatabase();

    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload(
                "{\"command\":\"create vertex set name = ?\",\"parameters\":[\"Jay\"]}",
                CONTENT.TEXT)
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload(
                "{\"command\":\"create vertex set name = ?\",\"parameters\":[\"Amiga\"]}",
                CONTENT.TEXT)
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    Assert.assertEquals(
        post("command/" + getDatabaseName() + "/sql/")
            .payload(
                "{\"command\":\"create edge from (select from v where name = 'Jay') to (select from v where name = 'Amiga')\"}",
                CONTENT.TEXT)
            .setUserName("admin")
            .setUserPassword("admin")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Override
  public String getDatabaseName() {
    return "httpgephi";
  }
}
