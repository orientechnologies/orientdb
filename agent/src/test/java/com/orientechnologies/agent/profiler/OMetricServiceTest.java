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

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.profiler.metrics.OMeter;
import com.orientechnologies.agent.profiler.metrics.OMetric;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.agent.services.backup.OBackupTask;
import com.orientechnologies.agent.services.backup.log.OBackupLogType;
import com.orientechnologies.agent.services.metrics.OGlobalMetrics;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsService;
import com.orientechnologies.agent.services.metrics.OrientDBMetricsSettings;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.junit.*;
import org.junit.rules.TestName;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Created by Enrico Risa on 22/03/16.
 */

public class OMetricServiceTest {

  private OServer server;

  @Rule
  public        TestName testName = new TestName();
  private final String   DB_NAME  = "backupDBTest";

  private OrientDBMetricsService metricsService;

  @Before
  public void bootOrientDB() throws Exception {

    InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);

    OrientDB orientDB = server.getContext();

    if (orientDB.exists(testName.getMethodName()))
      orientDB.drop(testName.getMethodName());

    orientDB.create(testName.getMethodName(), ODatabaseType.PLOCAL);

    server.activate();

    OEnterpriseAgent agent = server.getPluginByClass(OEnterpriseAgent.class);

    metricsService = agent.getServiceByClass(OrientDBMetricsService.class).get();

    OrientDBMetricsSettings settings = new OrientDBMetricsSettings();

    settings.enabled = true;
    settings.server.enabled = true;
    settings.database.enabled = true;

    metricsService.changeSettings(settings);
  }

  @Test
  public void crudOps() {

    ODatabaseSession db = server.getContext().open(testName.getMethodName(), "admin", "admin");

    OVertex v = db.newVertex("V");

    v.save();

    v.setProperty("name", "Foo");

    v.save();

    v.delete();

    String create = String.format(OGlobalMetrics.DATABASE_CREATE_OPS.name, testName.getMethodName());
    String read = String.format(OGlobalMetrics.DATABASE_READ_OPS.name, testName.getMethodName());
    String update = String.format(OGlobalMetrics.DATABASE_UPDATE_OPS.name, testName.getMethodName());
    String delete = String.format(OGlobalMetrics.DATABASE_DELETE_OPS.name, testName.getMethodName());

    Map<String, OMetric> metrics = metricsService.getRegistry().getMetrics();

    OMeter createMetrics = (OMeter) metrics.get(create);
    Assert.assertEquals(1, createMetrics.getCount());

    OMeter readMetrics = (OMeter) metrics.get(read);
    Assert.assertEquals(1, readMetrics.getCount());

    OMeter updateMetrics = (OMeter) metrics.get(update);
    Assert.assertEquals(1, updateMetrics.getCount());

    OMeter deleteMetrics = (OMeter) metrics.get(delete);
    Assert.assertEquals(1, deleteMetrics.getCount());
  }

  @After
  public void tearDownOrientDB() {
    OrientDB orientDB = server.getContext();
    if (orientDB.exists(testName.getMethodName()))
      orientDB.drop(testName.getMethodName());

    if (server != null)
      server.shutdown();

  }

}
