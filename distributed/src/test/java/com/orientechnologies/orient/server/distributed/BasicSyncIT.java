package com.orientechnologies.orient.server.distributed;

import com.google.gson.JsonSyntaxException;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.testutil.StatefulSetTemplateKeys;
import com.orientechnologies.orient.testutil.TestConfigs;
import com.orientechnologies.orient.testutil.TestSetup;
import io.kubernetes.client.PortForward;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.StringUtil;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;
import org.apache.commons.text.StringSubstitutor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BasicSyncIT {

  private TestSetup.setup setup;

  @Before
  public void before() throws Exception {
    setup = TestSetup.createSetup(new TestConfigs.SimpleDServer());
    for (String serverId: setup.getSetupConfig().getServerIds()) {
      // todo: replace with start all
      // catch setup error, especially, ApiException
      setup.startServer(serverId);
    }

    System.out.println("wait till instance is ready");
    Thread.sleep(90 * 1000);
    System.out.println("creating database...");

    OrientDB remote = new OrientDB(setup.getRemoteAddress(), "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
    remote.close();
    System.out.println("created database 'test'");
  }

  @Test
  public void sync()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException, InterruptedException,
      ApiException {
//    String remoteAddress = "remote:localhost";
//    String remoteAddress = "remote:" + nodeParams.get(0).get(StatefulSetTemplateKeys.ORIENTDB_BINARY_ADDRESS);
//    try (OrientDB remote = new OrientDB(remoteAddress, OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.createClass("One");
//        session.save(session.newElement("One"));
//        session.save(session.newElement("One"));
//      }
//      server2.shutdown();
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.save(session.newElement("One"));
//      }
//      System.out.println("saved session.");
//    }
    System.out.println("taking down instance 2");
    setup.shutdownServer(setup.getSetupConfig().getServerIds().get(1));
    System.out.println("waiting for scale down to settle!");
    Thread.sleep(10 * 1000);
//    server0.shutdown();
//    server1.shutdown();
//    // Starting the servers in reverse shutdown order to trigger miss sync
//    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
//    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
//    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
//    // Test server 0
//    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 1
//    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 2
//    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
  }

//  @Test
//  public void reverseStartSync()
//      throws ClassNotFoundException, InstantiationException, IllegalAccessException, IOException,
//          InterruptedException {
//    //    System.out.println("waiting...");
//    //    Thread.sleep(5 * 60 * 1000);
//
//    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.createClass("One");
//        session.save(session.newElement("One"));
//        session.save(session.newElement("One"));
//      }
//      server2.shutdown();
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        session.save(session.newElement("One"));
//      }
//    }
//    server0.shutdown();
//    server1.shutdown();
//    // Starting the servers in reverse shutdown order to trigger miss sync
//    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
//    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
//    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
//    // Test server 0
//    try (OrientDB remote = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 1
//    try (OrientDB remote = new OrientDB("remote:localhost:2425", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//    // Test server 2
//    try (OrientDB remote = new OrientDB("remote:localhost:2426", OrientDBConfig.defaultConfig())) {
//      try (ODatabaseSession session = remote.open("test", "admin", "admin")) {
//        assertEquals(session.countClass("One"), 3);
//      }
//    }
//  }

  @After
  public void after() throws InterruptedException, ApiException {
    setup.teardown();
  }
}
