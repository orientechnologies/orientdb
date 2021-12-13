package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SimpleDDLRestartDistributedIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;
  private OrientDB remote;
  private ODatabaseSession session;

  @Before
  public void before() throws Exception {
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    remote = new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    session = remote.open("test", "admin", "admin");
  }

  @Test
  public void test() throws Exception {
    session.command("create class one");
    server2.shutdown();
    session.command("create class testClass");
    server2.startupFromConfiguration();
    server2.activate();
    server2.getDistributedManager().waitUntilNodeOnline();
    try (OrientDB remoteServer2 =
        new OrientDB("remote:localhost:2426", "root", "test", OrientDBConfig.defaultConfig())) {
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

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    ODatabaseDocumentTx.closeAll();
  }
}
