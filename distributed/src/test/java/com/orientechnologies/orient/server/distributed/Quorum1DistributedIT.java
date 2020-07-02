package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Quorum1DistributedIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;
  private OrientDB remote;

  private static int THREADS = 50;
  private static int RECORDS_PER_THREAD = 20000;
//  private ODatabaseSession session;

  AtomicLong total = new AtomicLong(0);
  AtomicInteger done = new AtomicInteger(0);

  @Before
  public void before() throws Exception {
    server0 = OServer.startFromClasspathConfig("quorum1/orientdb-quorum1-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("quorum1/orientdb-quorum1-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("quorum1/orientdb-quorum1-dserver-config-2.xml");
    remote = new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    if (remote.exists("test")) {
      remote.drop("test");
    }
    remote.create("test", ODatabaseType.PLOCAL);
    ODatabaseSession session = remote.open("test", "admin", "admin");
    session.command("CREATE SEQUENCE TheSequence TYPE CACHED START 1 INCREMENT 1 CACHE 10");
    session.command("create class Foo");
    session.command("create property Foo.id LONG");
    session.command("create index Foo.id on Foo (id) UNIQUE");
    session.close();
  }

  class SequenceThread implements Runnable {
    @Override
    public void run() {
      try {
        ODatabaseSession session = remote.open("test", "admin", "admin");
        for (int i = 0; i < RECORDS_PER_THREAD; i++) {
          session.command("insert into Foo set id = sequence('TheSequence').next()");
          long inserted = total.incrementAndGet();
          if (inserted % 1000 == 0) {
            System.out.println("Inserted: " + inserted);
          }
        }
        done.incrementAndGet();
        session.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }


  @Test
  public void test() throws InterruptedException {

    ExecutorService service = Executors.newFixedThreadPool(THREADS);

    for (int i = 0; i < THREADS; i++) {
      service.submit(new SequenceThread());
    }
    service.shutdown();
    service.awaitTermination(160, TimeUnit.SECONDS);
    ODatabaseSession session = remote.open("test", "admin", "admin");
    try (OResultSet rs = session.query("select max(id) as max from Foo")) {
      long id = rs.next().getProperty("max");
      Assert.assertTrue(id >= THREADS * RECORDS_PER_THREAD);
    }
    Assert.assertEquals(THREADS, done.get());
  }


  @After
  public void after() throws InterruptedException {
    remote.drop("test");
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    ODatabaseDocumentTx.closeAll();
  }

}