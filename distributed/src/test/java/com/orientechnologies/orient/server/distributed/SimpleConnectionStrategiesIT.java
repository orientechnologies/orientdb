package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.setup.LocalTestSetup;
import com.orientechnologies.orient.setup.SetupConfig;
import com.orientechnologies.orient.setup.configs.SimpleDServerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_FETCH_HOST_LIST;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleConnectionStrategiesIT {

  private static LocalTestSetup setup;
  private static SetupConfig config;
  private static String server0, server1, server2;
  private static String databaseName = SimpleConnectionStrategiesIT.class.getSimpleName();

  @BeforeClass
  public static void before() {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    config = new SimpleDServerConfig();
    server0 = SimpleDServerConfig.SERVER0;
    server1 = SimpleDServerConfig.SERVER1;
    server2 = SimpleDServerConfig.SERVER2;
    setup = new LocalTestSetup(config);
    setup.setup();
    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.create(databaseName, ODatabaseType.PLOCAL);
    remote.close();
  }

  @AfterClass
  public static void after() throws InterruptedException {
    OrientDB remote = setup.createRemote(server0, "root", "test", OrientDBConfig.defaultConfig());
    remote.drop(databaseName);
    remote.close();

    setup.teardown();
    ODatabaseDocumentTx.closeAll();
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
    ODatabaseSession session = remote1.open(databaseName, "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session).getSessionMetadata().getServerUrl());

    ODatabaseSession session1 = remote1.open(databaseName, "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session1).getSessionMetadata().getServerUrl());
    session1.close();

    session.activateOnCurrentThread();
    session.close();
    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);

    Set<String> poolUrls = new HashSet<>();
    ODatabasePool pool = new ODatabasePool(remote1, databaseName, "admin", "admin");

    ODatabaseSession sessionP = pool.acquire();
    poolUrls.add(((ODatabaseDocumentRemote) sessionP).getSessionMetadata().getServerUrl());

    ODatabaseSession sessionP1 = pool.acquire();
    poolUrls.add(((ODatabaseDocumentRemote) sessionP1).getSessionMetadata().getServerUrl());
    sessionP1.close();
    sessionP.activateOnCurrentThread();
    sessionP.close();

    assertEquals(poolUrls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(poolUrls.stream().filter((x) -> x.contains("2425")).count(), 1);
  }

  @Test
  public void testConnectNoHostFetch() {
    OrientDB remote =
        setup.createRemote(
            server0,
            OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, false).build());
    ODatabaseSession session = remote.open(databaseName, "admin", "admin");
    assertEquals(
        ((OStorageRemote) ((ODatabaseDocumentInternal) session).getStorage())
            .getServerURLs()
            .size(),
        1);
    session.close();
    remote.close();

    OrientDB remote1 =
        setup.createRemote(
            server0,
            OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, true).build());
    ODatabaseSession session1 = remote1.open(databaseName, "admin", "admin");
    assertTrue(
        ((OStorageRemote) ((ODatabaseDocumentInternal) session1).getStorage())
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

    ODatabasePool pool = new ODatabasePool(remote, databaseName, "admin", "admin");
    ODatabaseSession session = pool.acquire();
    assertEquals(
        ((OStorageRemote) ((ODatabaseDocumentInternal) session).getStorage())
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
    ODatabasePool pool1 = new ODatabasePool(remote1, databaseName, "admin", "admin");
    ODatabaseSession session1 = pool1.acquire();
    assertTrue(
        ((OStorageRemote) ((ODatabaseDocumentInternal) session1).getStorage())
                .getServerURLs()
                .size()
            > 1);
    session1.close();
    pool1.close();
    remote1.close();
  }
}
