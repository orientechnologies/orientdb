/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePoolFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import junit.framework.Assert;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Insert records concurrently against the cluster
 */
public abstract class AbstractDistributedWriteTest extends AbstractServerClusterTest {
  protected static final int                      delayWriter = 0;
  protected static final int                      writerCount = 5;
  protected volatile int                          count       = 100;
  protected CountDownLatch                        runningWriters;
  protected final OPartitionedDatabasePoolFactory poolFactory = new OPartitionedDatabasePoolFactory();

  class Writer implements Callable<Void> {
    private final String databaseUrl;
    private int          serverId;
    private int          threadId;

    public Writer(final int iServerId, final int iThreadId, final String db) {
      serverId = iServerId;
      threadId = iThreadId;
      databaseUrl = db;
    }

    @Override
    public Void call() throws Exception {
      String name = Integer.toString(threadId);
      for (int i = 0; i < count; i++) {
        final ODatabaseDocumentTx database = poolFactory.get(databaseUrl, "admin", "admin").acquire();
        try {
          if ((i + 1) % 100 == 0)
            System.out.println("\nWriter " + threadId + "(" + database.getURL() + ") managed " + (i + 1) + "/" + count
                + " records so far");

          final ODocument person = createRecord(database, i);
          updateRecord(database, i);
          checkRecord(database, i);
          checkIndex(database, (String) person.field("name"), person.getIdentity());

          Thread.sleep(delayWriter);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + database.getURL());
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          System.out.println("Writer received exception (db=" + database.getURL());
          e.printStackTrace();
          break;
        } finally {
          database.close();
          runningWriters.countDown();
        }
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }

    private ODocument createRecord(ODatabaseDocumentTx database, int i) {
      final String uniqueId = serverId + "-" + threadId + "-" + i;

      ODocument person = new ODocument("Person").fields("id", UUID.randomUUID().toString(), "name", "Billy" + uniqueId, "surname",
          "Mayes" + uniqueId, "birthday", new Date(), "children", uniqueId);
      database.save(person);

      Assert.assertTrue(person.getIdentity().isPersistent());

      return person;
    }

    private void updateRecord(ODatabaseDocumentTx database, int i) {
      ODocument doc = loadRecord(database, i);
      doc.field("updated", true);
      doc.save();
    }

    private void checkRecord(ODatabaseDocumentTx database, int i) {
      ODocument doc = loadRecord(database, i);
      Assert.assertEquals(doc.field("updated"), Boolean.TRUE);
    }

    private void checkIndex(ODatabaseDocumentTx database, final String key, final ORID rid) {
      final List<OIdentifiable> result = database.command(new OCommandSQL("select from index:Person.name where key = ?")).execute(
          key);
      Assert.assertNotNull(result);
      Assert.assertEquals(result.size(), 1);
      Assert.assertNotNull(result.get(0).getRecord());
      Assert.assertEquals(((ODocument) result.get(0)).field("rid"), rid);
    }

    private ODocument loadRecord(ODatabaseDocumentTx database, int i) {
      final String uniqueId = serverId + "-" + threadId + "-" + i;

      List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from Person where name = 'Billy" + uniqueId
          + "'"));
      if (result.size() == 0)
        Assert.assertTrue("No record found with name = 'Billy" + uniqueId + "'!", false);
      else if (result.size() > 1)
        Assert.assertTrue(result.size() + " records found with name = 'Billy" + uniqueId + "'!", false);

      return result.get(0);
    }
  }

  public String getDatabaseName() {
    return "distributed";
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService writerExecutors = Executors.newCachedThreadPool();

    runningWriters = new CountDownLatch(serverInstance.size() * writerCount);

    int serverId = 0;
    int threadId = 0;
    List<Callable<Void>> writerWorkers = new ArrayList<Callable<Void>>();
    for (ServerRun server : serverInstance) {
      for (int j = 0; j < writerCount; j++) {
        Callable writer = createWriter(serverId, threadId++, getDatabaseURL(server));
        writerWorkers.add(writer);
      }
      serverId++;
    }
    List<Future<Void>> futures = writerExecutors.invokeAll(writerWorkers);

    System.out.println("Threads started, waiting for the end");

    for (Future<Void> future : futures) {
      future.get();
    }

    writerExecutors.shutdown();
    Assert.assertTrue(writerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All writer threads have finished, shutting down readers");
  }

  protected abstract String getDatabaseURL(ServerRun server);

  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new Writer(serverId, threadId, databaseURL);
  }
}
