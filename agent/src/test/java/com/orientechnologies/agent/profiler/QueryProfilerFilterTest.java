package com.orientechnologies.agent.profiler;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.profiler.metrics.OHistogram;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsService;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsSettings;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.SortedMap;

public class QueryProfilerFilterTest {

  private OServer server;

  private Integer                pageSize;
  private OEnterpriseAgent       agent;
  private OrientDBMetricsService metricsService;

  @Before
  public void init() throws Exception {

    server = OServer.startFromClasspathConfig("orientdb-server-config.xml");

    agent = server.getPluginByClass(OEnterpriseAgent.class);

    Optional<OrientDBMetricsService> serviceByClass = agent.getServiceByClass(OrientDBMetricsService.class);

    OrientDBMetricsSettings settings = new OrientDBMetricsSettings();

    settings.enabled = true;

    settings.database.enabled = true;

    metricsService = agent.getServiceByClass(OrientDBMetricsService.class).get();
    metricsService.changeSettings(settings);

    server.getContext().create(QueryProfilerFilterTest.class.getSimpleName(), ODatabaseType.PLOCAL);
  }

  @Test
  public void testFilterQueries() throws Exception {

    OrientDB context = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());

    ODatabaseSession local = context.open(QueryProfilerFilterTest.class.getSimpleName(), "admin", "admin");

    local.query("select from OUser \n where name = 'admin'").close();

    local.close();

    SortedMap<String, OHistogram> histograms = metricsService.getRegistry()
        .getHistograms((name, f) -> name.matches("(?s)db.*.query.*"));

    OHistogram histogram = histograms.get(
        String.format("db.%s.query.sql.select from OUser \n where name = 'admin'", QueryProfilerFilterTest.class.getSimpleName()));

    Assert.assertNotNull(histogram);

  }

  @After
  public void deInit() {

    server.getContext().drop(QueryProfilerFilterTest.class.getSimpleName());

    server.shutdown();
  }
}
