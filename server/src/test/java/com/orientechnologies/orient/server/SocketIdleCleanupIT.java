package com.orientechnologies.orient.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.orient.client.remote.ORemoteConnectionManager;
import com.orientechnologies.orient.client.remote.ORemoteConnectionPool;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SocketIdleCleanupIT {

  private OServer server;

  @Before
  public void before()
      throws IOException, InstantiationException, InvocationTargetException, NoSuchMethodException,
          MBeanRegistrationException, IllegalAccessException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, ClassNotFoundException, MalformedObjectNameException {
    String classpath = System.getProperty("java.class.path");
    System.out.println("Class path " + classpath);
    server =
        OServer.startFromStreamConfig(
            this.getClass().getResourceAsStream("orientdb-server-config.xml"));
  }

  @Test
  public void test() throws InterruptedException {
    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.CLIENT_CHANNEL_IDLE_CLOSE, true)
            .addConfig(OGlobalConfiguration.CLIENT_CHANNEL_IDLE_TIMEOUT, 1)
            .build();
    OrientDB orientdb = new OrientDB("remote:localhost", "root", "root", config);
    orientdb.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseSession session = orientdb.open("test", "admin", "admin");
    session.save(session.newVertex("V"));
    Thread.sleep(2000);
    OrientDBRemote remote = (OrientDBRemote) OrientDBInternal.extract(orientdb);
    ORemoteConnectionManager connectionManager = remote.getConnectionManager();
    ORemoteConnectionPool pool =
        connectionManager.getPool(connectionManager.getURLs().iterator().next());
    assertFalse(pool.getPool().getResources().iterator().next().isConnected());
    try (OResultSet result = session.query("select from V")) {
      assertEquals(result.stream().count(), 1);
    }
  }

  @After
  public void after() {
    server.shutdown();
  }
}
