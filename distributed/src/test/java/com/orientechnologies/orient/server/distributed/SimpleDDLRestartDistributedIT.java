package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.TestSetup;
import com.orientechnologies.orient.setup.TestSetupUtil;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleDDLRestartDistributedIT {

  private OrientDB remote;
  private ODatabaseSession session;
  private TestSetup setup;

  @Before
  public void before() throws Exception {
    SimpleDServerConfig config = new SimpleDServerConfig();
    setup = TestSetupUtil.create(config);
    setup.setup();

    remote =
        setup.createRemote(
            SimpleDServerConfig.SERVER0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'admin' role admin)", "test");
    session = remote.open("test", "admin", "admin");
  }

  @Test
  public void test() throws Exception {
    session.command("create class one");
    setup.shutdownServer(SimpleDServerConfig.SERVER2);
    session.command("create class testClass");
    session.close();
    setup.startServer(SimpleDServerConfig.SERVER2);
    try (OrientDB remoteServer2 =
        setup.createRemote(SimpleDServerConfig.SERVER2, OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession server2Session = remoteServer2.open("test", "admin", "admin")) {
        try (OResultSet result = server2Session.query("select from one")) {}
        try (OResultSet result = server2Session.query("select from testClass")) {}
      }
    }
  }

  @After
  public void after() throws InterruptedException {
    remote.drop("test");
    remote.close();
    setup.teardown();
    ODatabaseDocumentTx.closeAll();
  }
}
