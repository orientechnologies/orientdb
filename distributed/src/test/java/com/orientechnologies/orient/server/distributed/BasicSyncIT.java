package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.test.configs.SimpleDServerConfig;
import com.orientechnologies.orient.test.util.TestConfig;
import com.orientechnologies.orient.test.util.TestSetup;
import com.orientechnologies.orient.test.util.TestSetupUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BasicSyncIT {

  private TestSetup setup;
  private TestConfig config;
  private String server0, server1, server2;

  @Before
  public void before() throws Exception {
    config = new SimpleDServerConfig();
    setup = TestSetupUtil.create(config);
    server0 = config.getServerIds().get(0);
    server1 = config.getServerIds().get(1);
    server2 = config.getServerIds().get(2);
    setup.start();

    OrientDB remote =
        new OrientDB(
            "remote:" + setup.getAddress(server0, TestSetup.PortType.BINARY),
            "root",
            "test",
            OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    remote.close();
    System.out.println("created database 'test'");
  }

  // todo: this needs a timeout, otherwise can get stuck sometime!
  private ODatabaseSession openRemoteWithRetry(
      OrientDB remote, String database, String user, String password) {
    int i = 1, max = 10;
    while (true) {
      try {
        System.out.printf("Trying (%d/%d) to open database %s.\n", i, max, database);
        return remote.open(database, user, password);
      } catch (ODatabaseException e) {
        if (i++ >= max) {
          throw e;
        }
        try {
          Thread.sleep(15000);
        } catch (InterruptedException interruptedException) {
        }
      }
    }
  }

  private void dropDatabaseWithRetry(OrientDB remote, String database) {
    int i = 1, max = 5;
    while (true) {
      try {
        System.out.printf("Trying (%d/%d) to drop database %s.\n", i, max, database);
        remote.drop(database);
        break;
      } catch (OStorageException e) {
        if (i++ >= max) {
          throw e;
        }
        try {
          Thread.sleep(10000);
        } catch (InterruptedException interruptedException) {
        }
      }
    }
  }

  @Test
  public void sync() {
    String remoteAddress = "remote:" + setup.getAddress(server0, TestSetup.PortType.BINARY);
    try (OrientDB remote = new OrientDB(remoteAddress, OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = openRemoteWithRetry(remote, "test", "admin", "admin")) {
        session.createClass("One");
        session.save(session.newElement("One"));
        session.save(session.newElement("One"));
        System.out.println("created class and elements.");
      }
      System.out.println("shutting down server2");
      setup.shutdownServer(server2);
      try (ODatabaseSession session = openRemoteWithRetry(remote, "test", "admin", "admin")) {
        session.save(session.newElement("One"));
        System.out.println("created another element.");
      }
    }

    System.out.println("shutting down server0 and server1");
    setup.shutdownServer(server0);
    setup.shutdownServer(server1);
    // Starting the servers in reverse shutdown order to trigger miss sync
    System.out.println("starting servers again");
    setup.startServer(server2);
    setup.startServer(server1);
    setup.startServer(server0);
    // Test server 0
    remoteAddress = "remote:" + setup.getAddress(server0, TestSetup.PortType.BINARY);
    try (OrientDB remote = new OrientDB(remoteAddress, OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = openRemoteWithRetry(remote, "test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 1
    String server1Address = "remote:" + setup.getAddress(server1, TestSetup.PortType.BINARY);
    try (OrientDB remote = new OrientDB(server1Address, OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = openRemoteWithRetry(remote, "test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
    // Test server 2
    String server2Address = "remote:" + setup.getAddress(server2, TestSetup.PortType.BINARY);
    try (OrientDB remote = new OrientDB(server2Address, OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = openRemoteWithRetry(remote, "test", "admin", "admin")) {
        assertEquals(session.countClass("One"), 3);
      }
    }
  }

  @Test
  @Ignore
  public void reverseStartSync() {
    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.createClass("One");
        session.save(session.newElement("One"));
        session.save(session.newElement("One"));
      }
      setup.shutdownServer(server2);
      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
        session.save(session.newElement("One"));
      }
    }
    setup.shutdownServer(server1);
    setup.shutdownServer(server0);
    // Starting the servers in reverse shutdown order to trigger miss sync
    setup.startServer(server2);
    setup.startServer(server1);
    setup.startServer(server0);
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
  public void after() {
    try {
      String address = "remote:" + setup.getAddress(config.getServerIds().get(0), TestSetup.PortType.BINARY);
      OrientDB remote = new OrientDB(address, "root", "test", OrientDBConfig.defaultConfig());
      dropDatabaseWithRetry(remote, "test");
      remote.close();
      System.out.println("dropped and closed!");
    } finally {
      setup.teardown();
      ODatabaseDocumentTx.closeAll();
    }
  }
}
