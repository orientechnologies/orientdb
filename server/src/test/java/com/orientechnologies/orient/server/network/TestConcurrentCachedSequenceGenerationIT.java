package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConcurrentCachedSequenceGenerationIT {
  static final int THREADS = 20;
  static final int RECORDS = 100;
  private OServer server;
  private OrientDB orientDB;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        TestConcurrentCachedSequenceGenerationIT.class.getSimpleName());
    ODatabaseSession databaseSession =
        orientDB.open(
            TestConcurrentCachedSequenceGenerationIT.class.getSimpleName(), "admin", "admin");
    databaseSession.execute(
        "sql",
        "CREATE CLASS TestSequence EXTENDS V;\n"
            + " CREATE SEQUENCE TestSequenceIdSequence TYPE CACHED CACHE 100;\n"
            + "CREATE PROPERTY TestSequence.id LONG (MANDATORY TRUE, default \"sequence('TestSequenceIdSequence').next()\");\n"
            + "CREATE INDEX TestSequence_id_index ON TestSequence (id BY VALUE) UNIQUE;");
    databaseSession.close();
  }

  @Test
  public void test() throws InterruptedException {
    AtomicLong failures = new AtomicLong(0);
    ODatabasePool pool =
        new ODatabasePool(
            orientDB,
            TestConcurrentCachedSequenceGenerationIT.class.getSimpleName(),
            "admin",
            "admin");
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < THREADS; i++) {
      Thread thread =
          new Thread() {
            @Override
            public void run() {
              ODatabaseSession db = pool.acquire();
              try {
                for (int j = 0; j < RECORDS; j++) {
                  OVertex vert = db.newVertex("TestSequence");
                  assertNotNull(vert.getProperty("id"));
                  db.save(vert);
                }
              } catch (Exception e) {
                failures.incrementAndGet();
                e.printStackTrace();
              } finally {
                db.close();
              }
            }
          };
      threads.add(thread);
      thread.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    Assert.assertEquals(0, failures.get());
  }

  @After
  public void after() {
    orientDB.drop(TestConcurrentCachedSequenceGenerationIT.class.getSimpleName());
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Orient.instance().startup();
  }
}
