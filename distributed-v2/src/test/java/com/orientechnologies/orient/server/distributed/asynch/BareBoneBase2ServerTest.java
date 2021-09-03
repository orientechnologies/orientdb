package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import java.io.File;

public abstract class BareBoneBase2ServerTest extends BareBoneBase2ClientTest {

  protected static final String DB2_DIR = "target/db2";

  protected String getLocalURL2() {
    return "plocal:" + DB2_DIR + "/databases/" + getDatabaseName();
  }

  protected String getRemoteURL2() {
    return "remote:localhost:2425/" + getDatabaseName();
  }

  public void setUp() {
    System.out.println("SETUP TEST");
    super.setUp();
    OFileUtils.deleteRecursively(new File(DB2_DIR));
  }

  @Override
  protected void tearDown() throws Exception {
    System.out.println("TEAR DOWN");

    super.tearDown();

    // delete the folder should be enough if shutdown is done correctly
    // new ODatabaseDocumentTx(getLocalURL2()).open("admin", "admin").drop();
    OFileUtils.deleteRecursively(new File(DB2_DIR));
  }

  public void testReplication() throws Throwable {
    Orient.setRegisterDatabaseByPath(true);

    final BareBonesServer[] servers = new BareBonesServer[2];
    Thread dbServer1 =
        new Thread() {
          @Override
          public void run() {
            servers[0] = dbServer(DB1_DIR, getDatabaseName(), "asynch-dserver-config-0.xml");
          }
        };
    dbServer1.start();
    dbServer1.join();

    Thread dbServer2 =
        new Thread() {
          @Override
          public void run() {
            servers[1] = dbServer(DB2_DIR, getDatabaseName(), "asynch-dserver-config-1.xml");
          }
        };
    dbServer2.start();
    dbServer2.join();

    Thread dbClient1 =
        new Thread() {
          @Override
          public void run() {
            dbClient1(servers);
          }
        };
    dbClient1.start();

    Thread dbClient2 =
        new Thread() {
          @Override
          public void run() {
            dbClient2(servers);
          }
        };
    dbClient2.start();

    dbClient1.join();
    dbClient2.join();

    endTest(servers);
  }
}
