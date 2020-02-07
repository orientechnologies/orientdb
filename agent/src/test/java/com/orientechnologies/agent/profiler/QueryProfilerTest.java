package com.orientechnologies.agent.profiler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class QueryProfilerTest {

  private OServer server;

  private Integer pageSize;

  @Before
  public void init() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");
    server.getContext().create(QueryProfilerTest.class.getSimpleName(), ODatabaseType.MEMORY);
  }

  @Test
  public void testListQueries() throws Exception {

    pageSize = OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValue();

    OGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(1);

    OrientDB context = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch endLatch = new CountDownLatch(1);

    new Thread(() -> {
      ODatabaseSession local = context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

      OResultSet result = local.query("select from OUser");

      startLatch.countDown();

      try {
        endLatch.await();
        result.close();
      } catch (InterruptedException e) {
        OLogManager.instance().warn(this, "Thread interrupted: " + e.getMessage(), e);
      }

    }).start();

    ODatabaseSession db = context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

    startLatch.await();

    OResultSet resultSet = db.command("select listQueries() as queries");

    Collection queries = (Collection) resultSet.stream().findFirst().map((r) -> r.getProperty("queries")).get();

    resultSet.close();

    startLatch.countDown();

    Assert.assertEquals(2, queries.size());
  }

  @Test
  public void testListAndKillSessions() {

    OrientDB context = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

    ODatabaseSession local = context.open(QueryProfilerTest.class.getSimpleName(), "admin", "admin");

    Integer sessionId = -1;

    try (OResultSet resultSet = local.command("select listSessions() as sessions")) {

      List<OResult> sessions = (List<OResult>) resultSet.stream().findFirst().map((r) -> r.getProperty("sessions")).get();

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

      Collection sessions = (Collection) resultSet.stream().findFirst().map((r) -> r.getProperty("sessions")).get();

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
