package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Test HTTP "query" command.
 *
 * @author Luca Garulli (l.garulli--at-orientechnologies.com)
 */

public abstract class BaseHttpDatabaseTest extends BaseHttpTest {
  @Before
  public void createDatabase() throws Exception {
    ODatabaseRecordThreadLocal.INSTANCE.remove();

    super.startServer();
    Assert.assertEquals(
        post("database/" + getDatabaseName() + "/memory").setUserName("root").setUserPassword("root").getResponse().getStatusLine()
            .getStatusCode(), 200);

    onAfterDatabaseCreated();
  }

  @After
  public void dropDatabase() throws Exception {
    Assert.assertEquals(
        delete("database/" + getDatabaseName()).setUserName("root").setUserPassword("root").getResponse().getStatusLine()
            .getStatusCode(), 204);
    super.stopServer();

    onAfterDatabaseDropped();
  }

  protected void onAfterDatabaseCreated() throws Exception {
  }

  protected void onAfterDatabaseDropped() throws Exception {
  }

}
