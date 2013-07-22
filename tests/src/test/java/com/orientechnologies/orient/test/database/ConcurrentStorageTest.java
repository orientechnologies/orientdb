package com.orientechnologies.orient.test.database;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author edegtyarenko
 * @since 08.04.13 10:51
 */
@Test
public class ConcurrentStorageTest {

  private final String url = "plocal:target/ConcurrentStorageTest";

  @BeforeClass
  public void setUpClass() {
    final ODatabaseDocumentTx setup = new ODatabaseDocumentTx(url);

    if (setup.exists()) {
      setup.open("admin", "admin");
      setup.drop();
      setup.create();
    } else {
      setup.create();
    }

    try {
      if (setup.getMetadata().getSchema().getClass("test_class") != null) {
        setup.getMetadata().getSchema().dropClass("test_class");
      }
      final OClass testClass = setup.getMetadata().getSchema().createClass("test_class");
      testClass.createProperty("thread", OType.STRING);
      testClass.createProperty("time", OType.LONG).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      setup.getMetadata().getSchema().save();

      assertTrue(setup.getClusterIdByName("test_class") != -1);
      assertTrue(countInsertedRecords(setup) == 0);
    } finally {
      setup.close();
    }
  }

  @Test
  public void testStorageConcurrently() throws Exception {

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean test = new AtomicBoolean(true);
    final Deque<ODocument> data = new LinkedBlockingDeque<ODocument>();

    final List<Thread> testThreads = new ArrayList<Thread>();
    testThreads.add(new Thread(new Runnable() {
      @Override
      public void run() {
        final ODatabaseDocumentTx connection = createConnection();
        try {
          final int testClusterId = connection.getClusterIdByName("test_class");
          assertTrue(testClusterId != -1);

          latch.await();

          for (int i = 0; i < 1000; i++) {
            if (url.startsWith("remote")) {
              new OServerAdmin(url).connect("admin", "admin").freezeCluster(testClusterId, "plocal");
            } else {
              connection.freezeCluster(testClusterId);
            }
            System.out.println("frozen " + System.currentTimeMillis());
            final long startRecords = countInsertedRecords(connection);

            TimeUnit.MILLISECONDS.sleep(500);

            final long endRecords = countInsertedRecords(connection);

            assertEquals(startRecords, endRecords);

            System.out.println("released " + System.currentTimeMillis());
            if (url.startsWith("remote")) {
              new OServerAdmin(url).connect("admin", "admin").releaseCluster(testClusterId, "plocal");
            } else {
              connection.releaseCluster(testClusterId);
            }

            TimeUnit.MILLISECONDS.sleep(500);
          }
        } catch (Throwable e) {
          throw new RuntimeException(e);
        } finally {
          test.set(false);
          connection.close();
        }
      }
    }));
    for (int i = 0; i < 5; i++) {
      testThreads.add(new Thread(new Runnable() {
        @Override
        public void run() {
          final ODatabaseDocumentTx connection = createConnection();
          try {
            final Random random = new Random();
            final String name = Thread.currentThread().getName();
            latch.await();

            while (test.get()) {
              final long time = System.currentTimeMillis();
              ODocument doc = connection.newInstance("test_class").field("thread", name).field("time", time);
              doc = connection.save(doc);

              if (random.nextBoolean())
                data.addFirst(doc);
              else
                data.addLast(doc);

              System.out.println("create " + name + " " + time);
              TimeUnit.MILLISECONDS.sleep(25);
            }
          } catch (Throwable e) {
            test.set(false);
            throw new RuntimeException(e);
          } finally {
            test.set(false);
            connection.close();
          }
        }
      }));
    }
    testThreads.add(new Thread(new Runnable() {
      @Override
      public void run() {
        final ODatabaseDocumentTx connection = createConnection();
        try {
          final Random random = new Random();
          final String name = Thread.currentThread().getName();
          latch.await();

          while (test.get()) {
            final long time = System.currentTimeMillis();

            ODocument doc;
            if (random.nextBoolean())
              doc = data.pollFirst();
            else
              doc = data.pollLast();

            if (doc != null) {
              connection.delete(doc);
              System.out.println("delete " + name + " " + time);
            }

            TimeUnit.MILLISECONDS.sleep(25);
          }
        } catch (Throwable e) {
          test.set(false);
          throw new RuntimeException(e);
        } finally {
          test.set(false);
          connection.close();
        }
      }
    }));
    for (int i = 0; i < 5; i++) {
      testThreads.add(new Thread(new Runnable() {
        @Override
        public void run() {
          final ODatabaseDocumentTx connection = createConnection();
          try {
            final String name = Thread.currentThread().getName();
            latch.await();

            while (test.get()) {
              final long time = System.currentTimeMillis();

              for (ODocument doc : data) {
                connection.load(doc);
                System.out.println("read " + name + " " + time);
                TimeUnit.MILLISECONDS.sleep(25);
              }

            }
          } catch (Throwable e) {
            test.set(false);
            throw new RuntimeException(e);
          } finally {
            test.set(false);
            connection.close();
          }
        }
      }));
    }

    for (Thread testThread : testThreads) {
      testThread.start();
    }

    latch.countDown();

    for (Thread testThread : testThreads) {
      testThread.join();
    }

  }

  private long countInsertedRecords(ODatabaseDocumentTx connection) {
    final List<ODocument> result = connection.query(new OSQLSynchQuery<ODocument>("select count(*) from test_class"));
    return result.get(0).<Long> field("count");
  }

  private ODatabaseDocumentTx createConnection() {
    return new ODatabaseDocumentTx(url).open("admin", "admin");
  }
}
