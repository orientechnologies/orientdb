package com.orientechnologies.orient.server.distributed;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_FETCH_HOST_LIST;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.client.remote.ORemoteClient;
import com.orientechnologies.orient.client.remote.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.setup.LocalTestSetup;
import com.orientechnologies.orient.setup.ServerRun;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SimpleConnectionStrategiesIT {

  private static LocalTestSetup setup;
  private static SetupConfig config;
  private static String server0, server1, server2;
  private static String databaseName = SimpleConnectionStrategiesIT.class.getSimpleName();

  @BeforeClass
  public static void before() {
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = new LocalTestSetup(config);
    setup.setup();
    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.execute(
        "create database ? plocal users(admin identified by 'adminpwd' role admin)", databaseName);
    remote.close();
  }

  @AfterClass
  public static void after() throws InterruptedException {
    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.drop(databaseName);
    remote.close();

    setup.teardown();
  }

  @Test
  public void testRoundRobinOpenClose() {
    OrientDB remote1 =
        new OrientDB(
            "remote:localhost;localhost:2425",
            "root",
            "test",
            OrientDBConfig.builder()
                .addConfig(CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
                .build());
    Set<String> urls = new HashSet<>();
    ODatabaseSession session =
        remote1.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "adminpwd");
    urls.add(((ODatabaseDocumentRemote) session).getSession().getDebugLastHost());
    session.close();

    ODatabaseSession session1 =
        remote1.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "adminpwd");
    urls.add(((ODatabaseDocumentRemote) session1).getSession().getDebugLastHost());
    session1.close();

    assertEquals(urls.stream().count(), 2);

    remote1.close();
  }

  @Test
  public void testRoundRobin() {
    List<String> ids = Arrays.asList(server0, server1);
    OrientDB remote1 =
        setup.createRemote(
            ids,
            "root",
            "test",
            OrientDBConfig.builder()
                .addConfig(CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
                .build());
    Set<String> urls = new HashSet<>();
    ODatabaseSession session = remote1.open(databaseName, "admin", "adminpwd");
    urls.add(((ODatabaseDocumentRemote) session).getSession().getDebugLastHost());

    ODatabaseSession session1 = remote1.open(databaseName, "admin", "adminpwd");
    urls.add(((ODatabaseDocumentRemote) session1).getSession().getDebugLastHost());
    session1.close();

    session.activateOnCurrentThread();
    session.close();
    assertEquals(urls.stream().count(), 2);

    Set<String> poolUrls = new HashSet<>();

    try (ODatabasePool pool = new ODatabasePool(remote1, databaseName, "admin", "adminpwd")) {

      ODatabaseSession sessionP = pool.acquire();
      poolUrls.add(((ODatabaseDocumentRemote) sessionP).getSession().getDebugLastHost());

      ODatabaseSession sessionP1 = pool.acquire();
      poolUrls.add(((ODatabaseDocumentRemote) sessionP1).getSession().getDebugLastHost());
      sessionP1.close();
      sessionP.activateOnCurrentThread();
      sessionP.close();
    }
    assertEquals(poolUrls.stream().count(), 2);
    remote1.close();
  }

  @Test
  public void testRoundRobinSession() {
    OrientDB remote1 =
        new OrientDB(
            "remote:localhost;localhost:2425",
            "root",
            "test",
            OrientDBConfig.builder()
                .addConfig(CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_REQUEST")
                .build());
    Set<String> urls = new HashSet<>();
    ODatabaseSession session = remote1.open(databaseName, "admin", "adminpwd");
    session.query("select count(*) from ORole").close();
    urls.add(((ODatabaseDocumentRemote) session).getSession().getDebugLastHost());

    session.query("select count(*) from ORole").close();
    urls.add(((ODatabaseDocumentRemote) session).getSession().getDebugLastHost());

    session.close();
    assertEquals(urls.stream().count(), 2);
    remote1.close();
  }

  @Test
  public void testConnectNoHostFetch() {
    OrientDB remote =
        setup.createRemote(
            server0,
            OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, false).build());
    ODatabaseSession session = remote.open(databaseName, "admin", "adminpwd");
    assertEquals(
        ((ORemoteClient) ((ODatabaseDocumentRemote) session).getRemoteClient())
            .getServerURLs()
            .size(),
        1);
    session.close();
    remote.close();

    OrientDB remote1 =
        setup.createRemote(
            server0,
            OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, true).build());
    ODatabaseSession session1 = remote1.open(databaseName, "admin", "adminpwd");
    assertTrue(
        ((ORemoteClient) ((ODatabaseDocumentRemote) session1).getRemoteClient())
                .getServerURLs()
                .size()
            > 1);
    session1.close();
    remote1.close();
  }

  @Test
  public void testConnectNoHostFetchWithPool() {
    OrientDB remote =
        setup.createRemote(
            server0,
            OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, false).build());

    ODatabasePool pool = new ODatabasePool(remote, databaseName, "admin", "adminpwd");
    ODatabaseSession session = pool.acquire();
    assertEquals(
        ((ORemoteClient) ((ODatabaseDocumentRemote) session).getRemoteClient())
            .getServerURLs()
            .size(),
        1);
    session.close();
    pool.close();
    remote.close();

    OrientDB remote1 =
        setup.createRemote(
            server0,
            OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, true).build());
    ODatabasePool pool1 = new ODatabasePool(remote1, databaseName, "admin", "adminpwd");
    ODatabaseSession session1 = pool1.acquire();
    assertTrue(
        ((ORemoteClient) ((ODatabaseDocumentRemote) session1).getRemoteClient())
                .getServerURLs()
                .size()
            > 1);
    session1.close();
    pool1.close();
    remote1.close();
  }

  @Test
  public void testRoundRobinShutdown() throws Exception {
    OrientDB remote1 =
        new OrientDB(
            "remote:localhost;localhost:2425;localhost:2426",
            "root",
            "test",
            OrientDBConfig.builder()
                .addConfig(CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT")
                .build());
    Set<String> urls = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      ODatabaseSession session =
          remote1.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "adminpwd");
      urls.add(((ODatabaseDocumentRemote) session).getSession().getDebugLastHost());
      session.close();
    }

    assertTrue(urls.stream().count() >= 3);

    ServerRun toStop = setup.getServer(server1);
    toStop.shutdown();
    toStop.getServerInstance().waitForShutdown();
    urls.clear();

    for (int i = 0; i < 10; i++) {
      ODatabaseSession session2 =
          remote1.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "adminpwd");
      session2.query("select from OUSer").close();
      urls.add(((ODatabaseDocumentRemote) session2).getSession().getDebugLastHost());
      session2.close();
    }

    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 0);
    assertTrue(urls.stream().filter((x) -> x.contains("2424")).count() >= 1);

    remote1.close();
    toStop.startServer("orientdb-simple-dserver-config-1.xml");
    toStop
        .getServerInstance()
        .getDistributedManager()
        .waitUntilNodeOnline(
            toStop.getServerInstance().getDistributedManager().getLocalNodeName(),
            SimpleConnectionStrategiesIT.class.getSimpleName());
  }
}
