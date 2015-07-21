package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests HTTP "listDatabases" command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpListDatabasesTest extends BaseHttpTest {
  @Test
  public void testListDatabasesRootUser() throws Exception {
    Assert.assertEquals(setUserName("root").setUserPassword("root").get("listDatabases").getResponse().getStatusLine()
        .getStatusCode(), 200);
  }

  @Test
  public void testListDatabasesGuestUser() throws Exception {
    Assert.assertEquals(setUserName("guest").setUserPassword("guest").get("listDatabases").getResponse().getStatusLine()
                          .getStatusCode(), 200);
  }

  @Override
  public String getDatabaseName() {
    return "-";
  }

  @BeforeClass
  protected void startServer() throws Exception {
    super.startServer();
  }

  @AfterClass
  protected void stopServer() throws Exception {
    super.stopServer();
  }
}
