package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests HTTP errors command.
 * 
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public class HttpErrorsTest extends BaseHttpTest {
  @Test
  public void testCommandNotFound() throws Exception {
    Assert.assertEquals(setUserName("root").setUserPassword("root").get("commandNotfound").getResponse().getStatusLine()
        .getStatusCode(), 405);
  }

  @Test
  public void testCommandWrongMethod() throws Exception {
    Assert.assertEquals(setUserName("root").setUserPassword("root").post("listDatabases").getResponse().getStatusLine()
        .getStatusCode(), 405);
  }

  @Override
  public String getDatabaseName() {
    return "httperrors";
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
