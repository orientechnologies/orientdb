package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.io.File;
import junit.framework.TestCase;

public abstract class BareBoneBase1ClientTest extends TestCase {

  protected static final String CONFIG_DIR = "src/test/resources";
  protected static final String DB1_DIR = "target/db1";

  protected volatile Throwable exceptionInThread;

  protected abstract void dbClient1(BareBonesServer[] servers);

  protected abstract String getDatabaseName();

  protected String getRemoteURL() {
    return "remote:localhost:2424/" + getDatabaseName();
  }

  protected String getLocalURL() {
    return "plocal:" + DB1_DIR + "/databases/" + getDatabaseName();
  }

  public void setUp() {
    OFileUtils.deleteRecursively(new File(DB1_DIR));
  }

  @Override
  protected void tearDown() throws Exception {
    ODatabaseDocumentTx.closeAll();
    OFileUtils.deleteRecursively(new File(DB1_DIR));
  }

  public void testReplication() throws Throwable {
    Orient.setRegisterDatabaseByPath(true);

    final BareBonesServer[] servers = new BareBonesServer[1];
    // Start the first DB server.
    Thread dbServer1 =
        new Thread() {
          @Override
          public void run() {
            servers[0] = dbServer(DB1_DIR, getLocalURL(), "asynch-dserver-config-0.xml");
          }
        };
    dbServer1.start();
    dbServer1.join();

    // Start the first DB client.
    Thread dbClient1 =
        new Thread() {
          @Override
          public void run() {
            dbClient1(servers);
          }
        };
    dbClient1.start();
    dbClient1.join();

    endTest(servers);
  }

  protected void endTest(BareBonesServer[] servers) throws Throwable {
    for (BareBonesServer server : servers)
      if (server != null) {
        try {
          server.stop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    if (exceptionInThread != null) {
      throw exceptionInThread;
    }
  }

  protected BareBonesServer dbServer(String dbDirectory, String databaseName, String dbConfigName) {
    BareBonesServer dbServer = new BareBonesServer();
    dbServer.deleteRecursively(new File(dbDirectory));
    System.setProperty("ORIENTDB_HOME", dbDirectory);
    dbServer.start(CONFIG_DIR, dbConfigName);
    if (databaseName != null) {
      dbServer.createDB(databaseName);
    }

    return dbServer;
  }
}
