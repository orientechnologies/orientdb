package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_FETCH_HOST_LIST;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SimpleConnectionStrategiesIT {

  private OServer server0;
  private OServer server1;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    OrientDB remote = new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create(SimpleConnectionStrategiesIT.class.getSimpleName(), ODatabaseType.PLOCAL);
    remote.close();
  }

  @Test
  public void testRoundRobin() {
    OrientDB remote1 = new OrientDB("remote:localhost;localhost:2425", "root", "test",
        OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_STRATEGY, "ROUND_ROBIN_CONNECT").build());
    Set<String> urls = new HashSet<>();
    ODatabaseSession session = remote1.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session).getSessionMetadata().getServerUrl());

    ODatabaseSession session1 = remote1.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session1).getSessionMetadata().getServerUrl());
    session1.close();

    session.activateOnCurrentThread();
    session.close();
    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);

    Set<String> poolUrls = new HashSet<>();
    ODatabasePool pool = new ODatabasePool(remote1, SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");

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
    OrientDB remote = new OrientDB("remote:localhost",
        OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, false).build());
    ODatabaseSession session = remote.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    assertEquals(((OStorageRemote) ((ODatabaseDocumentInternal) session).getStorage()).getServerURLs().size(), 1);
    session.close();
    remote.close();

    OrientDB remote1 = new OrientDB("remote:localhost",
        OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, true).build());
    ODatabaseSession session1 = remote1.open(SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    assertTrue(((OStorageRemote) ((ODatabaseDocumentInternal) session1).getStorage()).getServerURLs().size() > 1);
    session1.close();
    remote1.close();

  }

  @Test
  public void testConnectNoHostFetchWithPool() {
    OrientDB remote = new OrientDB("remote:localhost",
        OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, false).build());

    ODatabasePool pool = new ODatabasePool(remote, SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    ODatabaseSession session = pool.acquire();
    assertEquals(((OStorageRemote) ((ODatabaseDocumentInternal) session).getStorage()).getServerURLs().size(), 1);
    session.close();
    pool.close();
    remote.close();

    OrientDB remote1 = new OrientDB("remote:localhost",
        OrientDBConfig.builder().addConfig(CLIENT_CONNECTION_FETCH_HOST_LIST, true).build());
    ODatabasePool pool1 = new ODatabasePool(remote1, SimpleConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    ODatabaseSession session1 = pool1.acquire();
    assertTrue(((OStorageRemote) ((ODatabaseDocumentInternal) session1).getStorage()).getServerURLs().size() > 1);
    session1.close();
    pool1.close();
    remote1.close();

  }

  @After
  public void after() throws InterruptedException {
    OrientDB remote = new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop(SimpleConnectionStrategiesIT.class.getSimpleName());
    remote.close();

    server0.shutdown();
    server1.shutdown();
    ODatabaseDocumentTx.closeAll();
  }

}