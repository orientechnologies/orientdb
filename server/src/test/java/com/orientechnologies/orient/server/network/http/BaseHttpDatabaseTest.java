package com.orientechnologies.orient.server.network.http;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test HTTP "query" command.
 *
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */
@Test
public abstract class BaseHttpDatabaseTest extends BaseHttpTest {
  @BeforeClass
  public void createDatabase() throws Exception {
    super.startServer();
    Assert.assertEquals(post("database/" + getDatabaseName() + "/memory").setUserName("root").setUserPassword("root").getResponse()
        .getStatusLine().getStatusCode(), 200);

    onAfterDatabaseCreated();
  }

  @AfterClass
  public void dropDatabase() throws Exception {
    Assert.assertEquals(delete("database/" + getDatabaseName()).setUserName("root").setUserPassword("root").getResponse()
        .getStatusLine().getStatusCode(), 204);
    super.stopServer();

    onAfterDatabaseDropped();
  }

  protected void onAfterDatabaseCreated() throws Exception {
  }

  protected void onAfterDatabaseDropped() throws Exception {
  }

}
