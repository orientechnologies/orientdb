package com.orientechnologies.orient.etl.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.http.OETLHandler;
import com.orientechnologies.orient.etl.http.OETLJob;
import com.orientechnologies.orient.etl.http.OETLListener;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ETLJobTest {

  private static final int TOTAL = 1000000;

  private OServer server0;

  @Before
  public void before() throws Exception {

    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
  }

  @Rule public TestName name = new TestName();

  @Test
  public void shouldLoadWithHandler() throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(1);
    OETLHandler handler =
        new OETLHandler(
            new OETLListener() {
              @Override
              public void onEnd(OETLJob oetlJob) {
                latch.countDown();
              }
            });
    String json =
        "{source: { content: { value: 'name,surname,@class\nJay,Miner,Person' } }, extractor : { csv: {} }, loader: { orientdb: {\n"
            + "      dbURL: 'memory:"
            + name.getMethodName()
            + "',\n"
            + "      dbUser: \"admin\",\n"
            + "      dbPassword: \"admin\",\n"
            + "      dbAutoCreate: true,\n      tx: false,\n"
            + "      batchCommit: 1000,\n"
            + "      wal : false,\n"
            + "      dbType: \"document\" , \"classes\": [\n"
            + "        {\n"
            + "          \"name\": \"Person\"\n"
            + "        },\n"
            + "        {\n"
            + "          \"name\": \"UpdateDetails\"\n"
            + "        }\n"
            + "      ]      } } }";

    ODocument cfg =
        new ODocument()
            .field("jsonConfig", json)
            .field("logLevel", 2)
            .field("outDBName", name.getMethodName())
            .field("configName", name.getMethodName());
    handler.executeImport(cfg, server0);

    Collection jobs = handler.status().field("jobs");

    Assert.assertEquals(1, jobs.size());

    latch.await(10000, TimeUnit.MILLISECONDS);

    ODatabaseSession session = server0.getContext().open(name.getMethodName(), "admin", "admin");

    assertThat(session.countClass("Person")).isEqualTo(1);
    session.close();
  }

  @After
  public void after() {

    final String buildDirectory =
        System.getProperty("buildDirectory", "./target")
            + File.separator
            + "server0"
            + File.separator
            + name.getMethodName()
            + File.separator
            + "etl-config";
    OFileUtils.deleteRecursively(Paths.get(buildDirectory).toFile());
    server0.dropDatabase(name.getMethodName());
    server0.shutdown();
  }
}
