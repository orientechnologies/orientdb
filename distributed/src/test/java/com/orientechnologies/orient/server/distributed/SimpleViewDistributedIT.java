package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleViewDistributedIT {

  private TestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;
  private String databaseName = SimpleViewDistributedIT.class.getSimpleName();

  @Before
  public void before() throws Exception {
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = TestSetupUtil.create(config);
    setup.setup();

    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'adminpwd' role admin)", databaseName);
    ODatabaseSession session = remote.open(databaseName, "admin", "adminpwd");
    session.createClass("test");
    session
        .command(
            "create view testView from (select name from test) metadata {updateIntervalSeconds:1}")
        .close();
    session.close();
    remote.close();
  }

  @Test
  public void testLiveQueryDifferentNode() throws InterruptedException {
    OrientDB remote1 = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session = remote1.open(databaseName, "admin", "adminpwd");
    for (int i = 0; i < 10000; i++) {
      OElement el = session.save(session.newElement("test"));
      el.setProperty("name", "name");
      session.save(el);
    }
    Thread.sleep(2000);
    OResultSet result = session.query("select count(*) from testView");
    long count = result.next().getProperty("count(*)");
    assertEquals(count, 10000);
    result.close();

    session.close();
    remote1.close();
  }

  @After
  public void after() throws InterruptedException {
    setup.teardown();
  }
}
