package com.orientechnologies.agent.profiler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.message.ORemoteResultSet;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryProfilerTest {

  private OServer server;

  private Integer pageSize;

  @Before
  public void init() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");
    server
        .getContext()
        .execute(
            "create database "
                + QueryProfilerTest.class.getSimpleName()
                + " memory users(admin identified by 'admin' role admin, reader identified by 'reader' role reader, writer identified by 'writer' role writer)");
  }

  @Test
  public void testListAndKillQueries() throws Exception {

    pageSize = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValue();

    OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(1);

    OrientDB context = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

    CountDownLatch startLatch = new CountDownLatch(2);
    CountDownLatch endLatch = new CountDownLatch(1);

    AtomicReference<String> queryId = new AtomicReference<>();

    new Thread(
            () -> {
              ODatabaseSession local =
                  context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

              OResultSet result = local.query("select from OUser");

              startLatch.countDown();

              try {
                endLatch.await();
                result.close();
              } catch (InterruptedException e) {
                OLogManager.instance().warn(this, "Thread interrupted: " + e.getMessage(), e);
              }
            })
        .start();

    new Thread(
            () -> {
              ODatabaseSession local =
                  context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

              OResultSet result = local.query("select from OUser");

              ORemoteResultSet remote = (ORemoteResultSet) result;

              queryId.set(remote.getQueryId());

              startLatch.countDown();

              try {
                endLatch.await();
                result.close();
              } catch (InterruptedException e) {
                OLogManager.instance().warn(this, "Thread interrupted: " + e.getMessage(), e);
              }
            })
        .start();

    ODatabaseSession db = context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

    startLatch.await();

    OResultSet resultSet = db.command("select listQueries() as queries");

    Collection<OResult> queries =
        (Collection) resultSet.stream().findFirst().map((r) -> r.getProperty("queries")).get();

    resultSet.close();

    Assert.assertEquals(3, queries.size());

    db.command("select killQuery(?) as queries", queryId.get()).close();

    resultSet = db.command("select listQueries() as queries");

    queries =
        (Collection) resultSet.stream().findFirst().map((r) -> r.getProperty("queries")).get();

    resultSet.close();

    Assert.assertEquals(2, queries.size());

    endLatch.countDown();
  }

  @Test
  public void testListAndKillSessions() {

    OrientDB context = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

    ODatabaseSession local =
        context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

    Integer sessionId = -1;

    try (OResultSet resultSet = local.command("select listSessions() as sessions")) {

      List<OResult> sessions =
          (List<OResult>)
              resultSet.stream().findFirst().map((r) -> r.getProperty("sessions")).get();

      Assert.assertEquals(1, sessions.size());

      sessionId = sessions.get(0).getProperty("connectionId");
    }

    local = context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

    try (OResultSet resultSet = local.command("select killSession(?) as sessions", sessionId)) {

    } catch (Exception e) {
      e.printStackTrace();
    }

    local.close();

    context.close();

    context = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

    local = context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

    try (OResultSet resultSet = local.command("select listSessions() as sessions")) {

      Collection sessions =
          (Collection) resultSet.stream().findFirst().map((r) -> r.getProperty("sessions")).get();

      // one is not connected
      Assert.assertEquals(2, sessions.size());
    }

    local.close();
  }

  @After
  public void deInit() {

    if (pageSize != null) {
      OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(pageSize);
    }

    server.shutdown();
  }
}
