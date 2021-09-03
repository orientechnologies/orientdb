package com.orientechnologies.orient.test.server.network.http;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests HTTP "listDatabases" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public class HttpListDatabasesTest extends BaseHttpTest {
  @Test
  public void testListDatabasesRootUser() throws Exception {
    Assert.assertEquals(
        setUserName("root")
            .setUserPassword("root")
            .get("listDatabases")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Test
  public void testListDatabasesGuestUser() throws Exception {
    Assert.assertEquals(
        setUserName("guest")
            .setUserPassword("guest")
            .get("listDatabases")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);
  }

  @Override
  public String getDatabaseName() {
    return "-";
  }

  @Before
  public void startServer() throws Exception {
    super.startServer();
  }

  @After
  public void stopServer() throws Exception {
    super.stopServer();
  }
}
