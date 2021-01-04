package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Test HTTP "query" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public abstract class BaseHttpDatabaseTest extends BaseHttpTest {
  @Before
  public void createDatabase() throws Exception {
    serverDirectory =
        Paths.get(System.getProperty("buildDirectory", "target"))
            .resolve(this.getClass().getSimpleName() + "Server")
            .toFile()
            .getCanonicalPath();

    super.startServer();
    ODocument pass = new ODocument();
    pass.setProperty("adminPassword", "admin");
    Assert.assertEquals(
        post("database/" + getDatabaseName() + "/memory")
            .payload(pass.toJSON(), CONTENT.JSON)
            .setUserName("root")
            .setUserPassword("root")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        200);

    onAfterDatabaseCreated();
  }

  @After
  public void dropDatabase() throws Exception {
    Assert.assertEquals(
        delete("database/" + getDatabaseName())
            .setUserName("root")
            .setUserPassword("root")
            .getResponse()
            .getStatusLine()
            .getStatusCode(),
        204);
    super.stopServer();

    onAfterDatabaseDropped();
  }

  protected void onAfterDatabaseCreated() throws Exception {}

  protected void onAfterDatabaseDropped() throws Exception {}
}
