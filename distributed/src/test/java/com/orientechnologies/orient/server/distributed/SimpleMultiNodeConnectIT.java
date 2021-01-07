package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleMultiNodeConnectIT {

  private TestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = TestSetupUtil.create(config);
    setup.setup();

    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", "test");
    ODatabaseSession session = remote.open("test", "admin", "admin");
    session.createClass("test");
    OElement doc = session.newElement("test");
    doc.setProperty("name", "value");
    session.save(doc);
    session.close();
    remote.close();
  }

  @Test
  public void testLiveQueryDifferentNode() {
    OrientDB remote1 =
        setup.createRemote(
            Arrays.asList(server0, server1), "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session = remote1.open("test", "admin", "admin");
    try (OResultSet result = session.query("select from test")) {
      assertEquals(1, result.stream().count());
    }
    setup.shutdownServer(server0);
    try (OResultSet result = session.query("select from test")) {
      assertEquals(1, result.stream().count());
    }
    remote1.close();
  }

  @After
  public void after() {
    setup.teardown();
  }
}
