package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

@Test(enabled = false)
public class OLiveIndexRebuildTest {
  private final OPartitionedDatabasePool pool = new OPartitionedDatabasePool("memory:liveIndexRebuild", "admin", "admin");

  private final String indexName    = "liveIndex";
  private final String className    = "liveIndexClass";
  private final String propertyName = "liveIndexProperty";

  private final String        databaseURL = "memory:liveIndexRebuild";
  private final AtomicBoolean stop        = new AtomicBoolean();

  public void testLiveIndexRebuild() throws Exception {
    final ODatabaseDocumentTx database = new ODatabaseDocumentTx(databaseURL);
    database.create();

    final OClass clazz = database.getMetadata().getSchema().createClass(className);
    clazz.createProperty(propertyName, OType.INTEGER);

    clazz.createIndex(indexName, OClass.INDEX_TYPE.UNIQUE, propertyName);

    for (int i = 0; i < 1000000; i++) {
      ODocument document = new ODocument(className);
      document.field(propertyName, i);
      document.save();
    }

    ExecutorService executorService = Executors.newFixedThreadPool(6);
    List<Future<?>> futures = new ArrayList<Future<?>>();

    for (int i = 0; i < 5; i++) {
      futures.add(executorService.submit(new Reader()));
    }

    futures.add(executorService.submit(new Writer()));

    Thread.sleep(60 * 60 * 1000);

    stop.set(true);
    executorService.shutdown();

    long minInterval = Long.MAX_VALUE;
    long maxInterval = Long.MIN_VALUE;

    for (Future<?> future : futures) {
      Object result = future.get();
      if (result instanceof long[]) {
        long[] results = (long[]) result;
        if (results[0] < minInterval)
          minInterval = results[0];

        if (results[1] > maxInterval)
          maxInterval = results[1];
      }
    }

    System.out.println("Min interval " + (minInterval / 1000000) + ", max interval " + (maxInterval / 1000000) + " ms");
  }

  private final class Writer implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      try {
        long rebuildInterval = 0;
        long rebuildCount = 0;
        while (!stop.get()) {
          for (int i = 0; i < 10; i++) {
            final ODatabaseDocumentTx database = pool.acquire();
            try {
              long start = System.nanoTime();
              database.command(new OCommandSQL("rebuild index " + indexName)).execute();
              long end = System.nanoTime();
              rebuildInterval += (end - start);
              rebuildCount++;
            } finally {
              database.close();
            }

            if (stop.get())
              break;
          }

          Thread.sleep(5 * 60 * 1000);
        }

        System.out.println("Average rebuild interval " + ((rebuildInterval / rebuildCount) / 1000000) + ", ms");
      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      }
      return null;
    }
  }

  private final class Reader implements Callable<long[]> {
    @Override
    public long[] call() throws Exception {
      long minInterval = Long.MAX_VALUE;
      long maxInterval = Long.MIN_VALUE;

      try {

        while (!stop.get()) {
          ODatabaseDocumentTx database = pool.acquire();
          try {
            long start = System.nanoTime();

            final List<ODocument> result = database
                .query(new OSQLSynchQuery<ODocument>("select from " + className + " where " + propertyName + " >= 100 and " +
                    propertyName + "< 200"));

            long end = System.nanoTime();
            long interval = end - start;

            if (interval > maxInterval)
              maxInterval = interval;

            if (interval < minInterval)
              minInterval = interval;

            Assert.assertEquals(result.size(), 100);
          } finally {
            database.close();
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        throw e;
      } catch (Error e) {
        e.printStackTrace();
        throw e;
      }

      return new long[] { minInterval, maxInterval };
    }
  }
}
