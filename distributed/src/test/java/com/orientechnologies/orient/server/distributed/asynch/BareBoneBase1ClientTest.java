package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import junit.framework.TestCase;

import java.io.File;

public abstract class BareBoneBase1ClientTest extends TestCase {

  protected static final String CONFIG_DIR = "src/test/resources";
  protected static final String DB1_DIR    = "target/db1";

  protected volatile Throwable  exceptionInThread;

  protected abstract void dbClient1();

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
    new ODatabaseDocumentTx(getLocalURL()).open("admin", "admin").drop();
    OFileUtils.deleteRecursively(new File(DB1_DIR));
  }

  public void testReplication() throws Throwable {
    Orient.setRegisterDatabaseByPath(true);

    final BareBonesServer[] servers = new BareBonesServer[1];
    // Start the first DB server.
    Thread dbServer1 = new Thread() {
      @Override
      public void run() {
        servers[0] = dbServer(DB1_DIR, getLocalURL(), "asynch-dserver-config-0.xml");
      }
    };
    dbServer1.start();
    dbServer1.join();

    // Start the first DB client.
    Thread dbClient1 = new Thread() {
      @Override
      public void run() {
        dbClient1();
      }
    };
    dbClient1.start();
    dbClient1.join();

    endTest(servers);
  }

  protected void endTest(BareBonesServer[] servers) throws Throwable {
    if (exceptionInThread != null) {
      throw exceptionInThread;
    }

    for (BareBonesServer server : servers)
      server.stop();
  }

  protected BareBonesServer dbServer(String dbDirectory, String orientUrl, String dbConfigName) {
    BareBonesServer dbServer = new BareBonesServer();
    dbServer.deleteRecursively(new File(dbDirectory));
    if (orientUrl != null) {
      dbServer.createDB(orientUrl);
    }
    System.setProperty("ORIENTDB_HOME", dbDirectory);
    dbServer.start(CONFIG_DIR, dbConfigName);

    return dbServer;
  }
}
