package com.orientechnologies.orient.server.distributed;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.CLIENT_CONNECTION_STRATEGY;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentRemote;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConnectionStrategiesIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create(ConnectionStrategiesIT.class.getSimpleName(), ODatabaseType.PLOCAL);
    remote.close();
  }

  @Test
  public void testRoundRobinShutdownWrite()
      throws InterruptedException, ClassNotFoundException, InstantiationException,
          IllegalAccessException, IOException {
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
        remote1.open(ConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session).getSessionMetadata().getDebugLastHost());
    session.close();

    ODatabaseSession session1 =
        remote1.open(ConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
    urls.add(((ODatabaseDocumentRemote) session1).getSessionMetadata().getDebugLastHost());
    session1.close();

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);

    server1.shutdown();
    server1.waitForShutdown();
    urls.clear();

    for (int i = 0; i < 10; i++) {
      ODatabaseSession session2 =
          remote1.open(ConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
      urls.add(((ODatabaseDocumentRemote) session2).getSessionMetadata().getDebugLastHost());
      for (int ji = 0; ji < 10; ji++) {
        session2.save(session2.newVertex());
      }
      session2.close();
    }

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);

    server1.startup(
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("orientdb-simple-dserver-config-1.xml"));
    server1.activate();
    server1.getDistributedManager().waitUntilNodeOnline();
    server1
        .getDistributedManager()
        .waitUntilNodeOnline(
            server1.getDistributedManager().getLocalNodeName(),
            ConnectionStrategiesIT.class.getSimpleName());

    for (int i = 0; i < 10; i++) {
      ODatabaseSession session2 =
          remote1.open(ConnectionStrategiesIT.class.getSimpleName(), "admin", "admin");
      urls.add(((ODatabaseDocumentRemote) session2).getSessionMetadata().getDebugLastHost());
      session2.close();
    }

    assertEquals(urls.stream().filter((x) -> x.contains("2424")).count(), 1);
    assertEquals(urls.stream().filter((x) -> x.contains("2425")).count(), 1);
    remote1.close();
  }

  @After
  public void after() throws InterruptedException {
    OrientDB remote =
        new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.drop(ConnectionStrategiesIT.class.getSimpleName());
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    ODatabaseDocumentTx.closeAll();
  }
}
