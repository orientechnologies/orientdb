package com.orientechnologies.orient.test.server.network.http;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.test.server.network.http.BaseHttpTest.CONTENT;
import java.io.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Test HTTP "query" command.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com) (l.garulli--at-orientdb.com)
 */
public abstract class BaseHttpDatabaseTest extends BaseHttpTest {

  private static String serverHome;
  private static String oldOrientDBHome;

  @BeforeClass
  public static void beforeTest() throws Exception {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + File.separator + BaseHttpTest.class.getSimpleName();

    File file = new File(serverHome);
    deleteDirectory(file);
    file = new File(serverHome);
    Assert.assertTrue(file.mkdir());

    oldOrientDBHome = System.getProperty("ORIENTDB_HOME");
    System.setProperty("ORIENTDB_HOME", serverHome);

    startServer();
  }

  @Before
  public void createDatabase() throws Exception {
    ODatabaseRecordThreadLocal.instance().remove();
    ODocument pass = new ODocument();
    pass.setProperty("adminPassword", "admin");
    Assert.assertEquals(
        post("database/" + getDatabaseName() + "/memory")
            .setUserName("root")
            .setUserPassword("root")
            .payload(pass.toJSON(), CONTENT.JSON)
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

    onAfterDatabaseDropped();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    stopServer();

    Orient.instance().shutdown();

    if (oldOrientDBHome != null) System.setProperty("ORIENTDB_HOME", oldOrientDBHome);
    else System.clearProperty("ORIENTDB_HOME");

    Thread.sleep(1000);
    ODatabaseDocumentTx.closeAll();

    File file = new File(serverHome);
    deleteDirectory(file);
    Orient.instance().startup();
  }

  protected void onAfterDatabaseCreated() throws Exception {}

  protected void onAfterDatabaseDropped() throws Exception {}

  protected static void deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      int len = files.length;

      for (int i = 0; i < len; ++i) {
        File file = files[i];
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }

      directory.delete();
    }

    if (directory.exists()) {
      throw new RuntimeException("unable to delete directory " + directory.getAbsolutePath());
    }
  }
}
