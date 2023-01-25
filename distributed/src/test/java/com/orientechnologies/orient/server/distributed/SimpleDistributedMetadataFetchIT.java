package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;

public class SimpleDistributedMetadataFetchIT {

  private TestSetup setup;
  private SetupConfig config;
  private String server0, server1, server2;

  private OrientDB remote;
  private ODatabaseSession session;

  @Before
  public void before() throws Exception {
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = TestSetupUtil.create(config);
    setup.setup();

    remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", "test");
    session = remote.open("test", "admin", "admin");
  }

  @Test
  public void test() {
    // Query with SQL
    OResultSet res = session.query("select from metadata:distributed");
    assertTrue(res.hasNext());
    assertNotNull(res.next().getProperty("servers"));
  }

  @After
  public void after() throws InterruptedException {
    try {
      if (remote != null) {
        remote.drop("test");
        remote.close();
      }
    } finally {
      setup.teardown();
      ODatabaseDocumentTx.closeAll();
    }
  }
}
