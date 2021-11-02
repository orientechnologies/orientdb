package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/** Created by tglman on 16/06/17. */
public class OLiveQueryRemoteTest {

  private OServer server;
  private OrientDB orientDB;
  private ODatabaseDocument database;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();
    orientDB = new OrientDB("remote:localhost:", "root", "root", OrientDBConfig.defaultConfig());
    orientDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OLiveQueryRemoteTest.class.getSimpleName());
    database = orientDB.open(OLiveQueryRemoteTest.class.getSimpleName(), "admin", "admin");
  }

  @After
  public void after() {
    database.close();
    orientDB.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Orient.instance().startup();
  }

  class MyLiveQueryListener implements OLiveQueryResultListener {

    public CountDownLatch latch;
    public CountDownLatch ended = new CountDownLatch(1);

    public MyLiveQueryListener(CountDownLatch latch) {
      this.latch = latch;
    }

    public List<OResult> ops = new ArrayList<OResult>();

    @Override
    public void onCreate(ODatabaseDocument database, OResult data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
      ops.add(after);
      latch.countDown();
    }

    @Override
    public void onDelete(ODatabaseDocument database, OResult data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onError(ODatabaseDocument database, OException exception) {}

    @Override
    public void onEnd(ODatabaseDocument database) {
      ended.countDown();
    }
  }

  @Test
  public void testRidSelect() throws InterruptedException {
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(1));
    OVertex item = database.newVertex();
    item.save();
    OLiveQueryMonitor live = database.live("LIVE SELECT FROM " + item.getIdentity(), listener);
    item.setProperty("x", "z");
    item.save();
    Assert.assertTrue(listener.latch.await(10, TimeUnit.SECONDS));
  }

  @Test
  public void testLiveInsert() throws InterruptedException {

    database.getMetadata().getSchema().createClass("test");
    database.getMetadata().getSchema().createClass("test2");
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(2));

    OLiveQueryMonitor monitor = database.live("select from test", listener);
    Assert.assertNotNull(monitor);

    database.command("insert into test set name = 'foo', surname = 'bar'").close();
    database.command("insert into test set name = 'foo', surname = 'baz'").close();
    database.command("insert into test2 set name = 'foo'").close();

    Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));

    monitor.unSubscribe();
    Assert.assertTrue(listener.ended.await(1, TimeUnit.MINUTES));

    database.command("insert into test set name = 'foo', surname = 'bax'");
    database.command("insert into test2 set name = 'foo'");
    database.command("insert into test set name = 'foo', surname = 'baz'");

    Assert.assertEquals(listener.ops.size(), 2);
    for (OResult doc : listener.ops) {
      Assert.assertEquals(doc.getProperty("@class"), "test");
      Assert.assertEquals(doc.getProperty("name"), "foo");
      ORID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }

  @Test
  @Ignore
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {
    OSchema schema = database.getMetadata().getSchema();
    OClass oRestricted = schema.getClass("ORestricted");
    schema.createClass("test", oRestricted);

    int liveMatch = 1;
    OResultSet query = database.query("select from OUSer where name = 'reader'");

    final OIdentifiable reader = query.next().getIdentity().orElse(null);
    final OIdentifiable current = database.getUser().getIdentity();

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch dataArrived = new CountDownLatch(1);
    Future<Integer> future =
        executorService.submit(
            new Callable<Integer>() {
              @Override
              public Integer call() throws Exception {
                ODatabaseDocument db =
                    orientDB.open(OLiveQueryRemoteTest.class.getSimpleName(), "reader", "reader");

                final AtomicInteger integer = new AtomicInteger(0);
                db.live(
                    "live select from test",
                    new OLiveQueryResultListener() {

                      @Override
                      public void onCreate(ODatabaseDocument database, OResult data) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onUpdate(
                          ODatabaseDocument database, OResult before, OResult after) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onDelete(ODatabaseDocument database, OResult data) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onError(ODatabaseDocument database, OException exception) {}

                      @Override
                      public void onEnd(ODatabaseDocument database) {}
                    });

                latch.countDown();
                Assert.assertTrue(dataArrived.await(2, TimeUnit.MINUTES));
                return integer.get();
              }
            });

    latch.await();

    query.close();
    database.command("insert into test set name = 'foo', surname = 'bar'");

    database.command(
        "insert into test set name = 'foo', surname = 'bar', _allow=?",
        new ArrayList<OIdentifiable>() {
          {
            add(current);
            add(reader);
          }
        });

    Integer integer = future.get();
    Assert.assertEquals(integer.intValue(), liveMatch);
  }

  @Test
  public void testBatchWithTx() throws InterruptedException {

    database.getMetadata().getSchema().createClass("test");
    database.getMetadata().getSchema().createClass("test2");

    int txSize = 100;

    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(txSize));

    OLiveQueryMonitor monitor = database.live("select from test", listener);
    Assert.assertNotNull(monitor);

    database.begin();
    for (int i = 0; i < txSize; i++) {
      OElement elem = database.newElement("test");
      elem.setProperty("name", "foo");
      elem.setProperty("surname", "bar" + i);
      elem.save();
    }
    database.commit();

    Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));

    Assert.assertEquals(listener.ops.size(), txSize);
    for (OResult doc : listener.ops) {
      Assert.assertEquals(doc.getProperty("@class"), "test");
      Assert.assertEquals(doc.getProperty("name"), "foo");
      ORID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }
}
