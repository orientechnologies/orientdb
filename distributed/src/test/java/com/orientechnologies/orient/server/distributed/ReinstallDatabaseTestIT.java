package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;


import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReinstallDatabaseTestIT {
  public static final String DATABASE_NAME = "ReinstallDatabaseTestIT";
  private OServer server0;
  private OServer server1;
  private OServer server2;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS.setValue(2);
    OGlobalConfiguration.DISTRIBUTED_LOCAL_QUEUESIZE.setValue(5);
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create("ReinstallDatabaseTestIT", ODatabaseType.PLOCAL);
    ODatabaseSession session = remote.open("ReinstallDatabaseTestIT", "admin", "admin");
    session.createClass("Person");
    session.createClass("Person1");
    OElement doc = session.newElement("Person");
    doc.setProperty("name", "value");
    session.save(doc);
    session.close();
    remote.close();
  }

  @Test
  public void testWritingWhileReinstall() throws InterruptedException {
    OrientDB remote1 =
        new OrientDB(
            "remote:localhost:2424;localhost:2425", "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session = remote1.open(DATABASE_NAME, "admin", "admin");
    try (OResultSet result = session.query("select from Person")) {
      assertEquals(1, result.stream().count());
    }
    CountDownLatch latch = new CountDownLatch(1);
    int first = 100;
    for (int i = 0; i < first; i++) {
      session.begin();
      OElement person = session.newElement("Person");
      person.setProperty("id", i);
      person.save();
      person = session.newElement("Person1");
      person.setProperty("id", i);
      person.save();
      session.commit();
    }
    new Thread(
            () -> {
              server2.getDistributedManager().installDatabase(false, DATABASE_NAME, true, true);
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e) {
              }
              latch.countDown();
            })
        .start();
    int second = 1000;
    for (int i = 0; i < second; i++) {
      session.begin();
      OElement person = session.newElement("Person");
      person.setProperty("id", i);
      person.save();
      person = session.newElement("Person1");
      person.setProperty("id", i);
      person.save();
      session.commit();
    }
    latch.await();
    int retry = 0;
    while (true) {
      ODistributedServerManager.DB_STATUS databaseStatus =
          server2
              .getDistributedManager()
              .getDatabaseStatus(server2.getDistributedManager().getLocalNodeName(), DATABASE_NAME);
      if (databaseStatus.equals(ODistributedServerManager.DB_STATUS.ONLINE) || retry > 10) {
        break;
      }
      Thread.sleep(1000);
      retry++;
    }
    try (OResultSet result = session.query("select from Person")) {
      assertEquals(first + second + 1, result.stream().count());
    }
    session.close();
    remote1.close();
    // TODO: this case is not yet sorted out, will be in next versions
    //    remote1 = new OrientDB("remote:localhost:2426", "root", "test",
    // OrientDBConfig.defaultConfig());
    //    session = remote1.open(DATABASE_NAME, "admin", "admin");
    //    try (OResultSet result = session.query("select from Person")) {
    //      assertEquals(first + second + 1, result.stream().count());
    //    }
  }

  @After
  public void after() {
    OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS.setValue(
        OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS.getDefValue());
    OGlobalConfiguration.DISTRIBUTED_LOCAL_QUEUESIZE.setValue(
        OGlobalConfiguration.DISTRIBUTED_LOCAL_QUEUESIZE.getDefValue());
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop("ReinstallDatabaseTestIT");
    remote.close();
    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    OFileUtils.deleteRecursively(new File(server0.getDatabaseDirectory()));
    OFileUtils.deleteRecursively(new File(server1.getDatabaseDirectory()));
    OFileUtils.deleteRecursively(new File(server2.getDatabaseDirectory()));
  }
}
