/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.profiler;

import com.opencsv.CSVReader;
import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.profiler.source.CSVAggregateReporter;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsService;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsSettings;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.server.distributed.http.EEBaseServerHttpTest;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.*;
import org.junit.rules.TestName;

public class OMetricCSVQueryAggregatorTest extends EEBaseServerHttpTest {

  private OEnterpriseServer eeServer;

  @Rule public TestName testName = new TestName();

  private OrientDBMetricsService metricsService;

  @Before
  public void bootOrientDB() {

    OEnterpriseAgent agent = server.getPluginByClass(OEnterpriseAgent.class);

    eeServer = agent.getEnterpriseServer();

    metricsService = agent.getServiceByClass(OrientDBMetricsService.class).get();

    OrientDBMetricsSettings settings = new OrientDBMetricsSettings();

    settings.enabled = true;
    settings.server.enabled = true;
    settings.database.enabled = true;

    metricsService.changeSettings(settings);

    remote.execute(
        "create database "
            + name.getMethodName()
            + " plocal users(admin identified by 'admin' role admin)");
  }

  @Test
  public void csvAggregatorTest() throws Exception {

    ODropWizardMetricsRegistry registry = (ODropWizardMetricsRegistry) metricsService.getRegistry();

    Path path = Files.createTempDirectory("csvAggregatorTest");

    CountDownLatch latch = new CountDownLatch(1);

    CSVAggregateReporter reporter =
        CSVAggregateReporter.forRegistry(eeServer, registry.getInternal())
            .withCallback(
                () -> {
                  latch.countDown();
                  return null;
                })
            .build(path.toFile());

    reporter.start(5000, TimeUnit.MILLISECONDS);

    ODatabaseSession db = server.getContext().open(testName.getMethodName(), "admin", "admin");

    db.query("select from OUser").close();

    latch.await();

    reporter.stop();

    CSVReader reader = new CSVReader(new FileReader(path.resolve("db.queries.csv").toFile()));

    List<String[]> rows = reader.readAll();

    Assert.assertEquals(2, rows.size());

    String[] first = rows.get(1);

    Assert.assertEquals(name.getMethodName(), first[1]);
    Assert.assertEquals("sql", first[2]);
    Assert.assertEquals("select from OUser", first[3]);

    reader.close();
  }

  @Override
  protected boolean shouldCreateDatabase() {
    return false;
  }
}
