package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.setup.LocalTestSetup;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReinstallDatabaseTestIT {
  public static final String DATABASE_NAME = "ReinstallDatabaseTestIT";
  // Relies on direct access to OServer to install DB and can run only on local setup.
  private LocalTestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    OGlobalConfiguration.DISTRIBUTED_DB_WORKERTHREADS.setValue(2);
    OGlobalConfiguration.DISTRIBUTED_LOCAL_QUEUESIZE.setValue(5);
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = new LocalTestSetup(config);
    setup.setup();

    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", DATABASE_NAME);
    ODatabaseSession session = remote.open(DATABASE_NAME, "admin", "admin");
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
    List<String> ids = Arrays.asList(server0, server1);
    OrientDB remote1 = setup.createRemote(ids, "root", "test", OrientDBConfig.defaultConfig());
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
    OServer server2Instance = setup.getServer(server2).getServerInstance();
    new Thread(
            () -> {
              server2Instance
                  .getDistributedManager()
                  .installDatabase(false, DATABASE_NAME, true, true);
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
          server2Instance
              .getDistributedManager()
              .getDatabaseStatus(
                  server2Instance.getDistributedManager().getLocalNodeName(), DATABASE_NAME);
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
    try {
      OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
      remote.drop(DATABASE_NAME);
      remote.close();
    } finally {
      setup.teardown();
    }
  }
}
