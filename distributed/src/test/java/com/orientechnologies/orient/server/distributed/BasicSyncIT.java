package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BasicSyncIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;

  @Before
  public void before() throws Exception {
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    remote.close();
  }

  private void waitForDbOnlineStatus(String dbName) throws InterruptedException {
    server0
        .getDistributedManager()
        .waitUntilNodeOnline(server0.getDistributedManager().getLocalNodeName(), dbName);
    server1
        .getDistributedManager()
        .waitUntilNodeOnline(server1.getDistributedManager().getLocalNodeName(), dbName);
    server2
        .getDistributedManager()
        .waitUntilNodeOnline(server2.getDistributedManager().getLocalNodeName(), dbName);
  }

  @Test
  public void sync()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
          InterruptedException {
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.createClass("One");
        session.save(session.newElement("One"));
        session.save(session.newElement("One"));
      }
      server2.shutdown();
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.save(session.newElement("One"));
      }
    }
    server0.shutdown();
    server1.shutdown();
    // Starting the servers in reverse shutdown order to trigger miss sync
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    waitForDbOnlineStatus("test");
    // Test server 0
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 1
    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 2
    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
  }

  @Test
  public void reverseStartSync()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
          InterruptedException {
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.createClass("One");
        session.save(session.newElement("One"));
        session.save(session.newElement("One"));
      }
      server2.shutdown();
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.save(session.newElement("One"));
      }
    }
    server0.shutdown();
    server1.shutdown();
    // Starting the servers in reverse shutdown order to trigger miss sync
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    waitForDbOnlineStatus("test");
    // Test server 0
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 1
    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 2
    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
  }

  @After
  public void after() throws InterruptedException {
    System.out.println("shutdown");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop("test");
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    ODatabaseDocumentTx.closeAll();
  }
}
