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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
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
public abstract class AbstractServerClusterInsertTest extends AbstractDistributedWriteTest {
  protected static final int delayWriter = 0;
  protected static final int delayReader = 1000;
  protected static final int writerCount = 5;
  protected int              baseCount   = 0;
  protected int              expected;
  protected OIndex<?>        idx;
  protected int              maxRetries  = 1;

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
        final ODatabaseDocumentTx database = new ODatabaseDocumentTx(databaseUrl).open("admin", "admin");

        try {
          final int id = baseCount + i;

          int retry = 0;

          for (retry = 0; retry < maxRetries; retry++) {
            try {
              final ODocument person = createRecord(database, id);
              updateRecord(database, id);
              checkRecord(database, id);
              checkIndex(database, (String) person.field("name"), person.getIdentity());

              if ((i + 1) % 100 == 0)
                System.out.println("\nWriter " + database.getURL() + " managed " + (i + 1) + "/" + count + " records so far");

              if (delayWriter > 0)
                Thread.sleep(delayWriter);

              // OK
              break;

            } catch (InterruptedException e) {
              System.out.println("Writer received interrupt (db=" + database.getURL());
              Thread.currentThread().interrupt();
              break;
            } catch (ORecordDuplicatedException e) {
              // IGNORE IT
            } catch (ONeedRetryException e) {
              System.out.println("Writer received exception (db=" + database.getURL());

              if (retry >= maxRetries)
                e.printStackTrace();

              break;
            } catch (ODistributedException e) {
              if (!(e.getCause() instanceof ORecordDuplicatedException)) {
                database.rollback();
                throw e;
              }
            } catch (Throwable e) {
              System.out.println("Writer received exception (db=" + database.getURL());
              e.printStackTrace();
              return null;
            }
          }
        } finally {
          runningWriters.countDown();
          database.close();
        }
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }

    private ODocument createRecord(ODatabaseDocumentTx database, int i) {
      final String uniqueId = serverId + "-" + threadId + "-" + i;

      // System.out.println("Creating person " + uniqueId);

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

  class Reader implements Callable<Void> {
    private final String databaseUrl;

    public Reader(final String db) {
      databaseUrl = db;
    }

    @Override
    public Void call() throws Exception {
      try {
        while (runningWriters.getCount() > 0) {
          try {
            printStats(databaseUrl);

            if (delayReader > 0)
              Thread.sleep(delayReader);

          } catch (Exception e) {
            break;
          }
        }

      } finally {
        printStats(databaseUrl);
      }
      return null;
    }
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    try {
      new ODocument("Customer").fields("name", "Jay", "surname", "Miner").save();
      new ODocument("Customer").fields("name", "Luke", "surname", "Skywalker").save();
      new ODocument("Provider").fields("name", "Yoda", "surname", "Nothing").save();
    } finally {
      database.close();
    }

    executeMultipleTest();
    dropIndexNode1();
    recreateIndexNode2();
  }

  protected void executeMultipleTest() throws InterruptedException, java.util.concurrent.ExecutionException {
    ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    try {
      List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
      baseCount = ((Number) result.get(0).field("count")).intValue();
    } finally {
      database.close();
    }

    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService writerExecutors = Executors.newCachedThreadPool();
    final ExecutorService readerExecutors = Executors.newCachedThreadPool();

    runningWriters = new CountDownLatch(serverInstance.size() * writerCount);

    int serverId = 0;
    int threadId = 0;
    List<Callable<Void>> writerWorkers = new ArrayList<Callable<Void>>();
    for (ServerRun server : serverInstance) {
      if (server.isActive()) {
        for (int j = 0; j < writerCount; j++) {
          Callable writer = createWriter(serverId, threadId++, getDatabaseURL(server));
          writerWorkers.add(writer);
        }
        serverId++;
      }
    }

    expected = writerCount * count * serverId + baseCount;

    List<Future<Void>> futures = writerExecutors.invokeAll(writerWorkers);

    List<Callable<Void>> readerWorkers = new ArrayList<Callable<Void>>();
    for (ServerRun server : serverInstance) {
      if (server.isActive()) {
        Callable<Void> reader = createReader(getDatabaseURL(server));
        readerWorkers.add(reader);
      }
    }

    List<Future<Void>> rFutures = readerExecutors.invokeAll(readerWorkers);

    System.out.println("Threads started, waiting for the end");

    for (Future<Void> future : futures) {
      future.get();
    }

    writerExecutors.shutdown();
    Assert.assertTrue(writerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All writer threads have finished, shutting down readers");

    for (Future<Void> future : rFutures) {
      future.get();
    }

    readerExecutors.shutdown();
    Assert.assertTrue(readerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All threads have finished, shutting down server instances");

    for (ServerRun server : serverInstance) {
      if (server.isActive()) {
        printStats(getDatabaseURL(server));
      }
    }

    checkInsertedEntries();
    checkIndexedEntries();
  }

  protected Callable<Void> createReader(String databaseURL) {
    return new Reader(databaseURL);
  }

  protected abstract String getDatabaseURL(ServerRun server);

  /**
   * Event called right after the database has been created and right before to be replicated to the X servers
   *
   * @param db
   *          Current database
   */
  @Override
  protected void onAfterDatabaseCreation(final OrientBaseGraph db) {
    System.out.println("Creating database schema...");

    // CREATE BASIC SCHEMA
    OClass personClass = db.getRawGraph().getMetadata().getSchema().createClass("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("name", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.STRING);

    final OSchema schema = db.getRawGraph().getMetadata().getSchema();
    OClass person = schema.getClass("Person");
    idx = person.createIndex("Person.name", INDEX_TYPE.UNIQUE, "name");

    OClass customer = schema.createClass("Customer", person);
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = schema.createClass("Provider", person);
    provider.createProperty("totalPurchased", OType.DECIMAL);
  }

  protected void dropIndexNode1() {
    ServerRun server = serverInstance.get(0);
    ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
    try {
      Object result = database.command(new OCommandSQL("drop index Person.name")).execute();
      System.out.println("dropIndexNode1: Node1 drop index: " + result);
    } finally {
      database.close();
    }

    // CHECK ON NODE 1
    server = serverInstance.get(1);
    database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
    try {
      database.getMetadata().getIndexManager().reload();
      Assert.assertNull(database.getMetadata().getIndexManager().getIndex("Person.name"));
      System.out.println("dropIndexNode1: Node2 hasn't the index too, ok");
    } finally {
      database.close();
    }
  }

  protected void recreateIndexNode2() {
    // RE-CREATE INDEX ON NODE 1
    ServerRun server = serverInstance.get(1);
    ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
    try {
      Object result = database.command(new OCommandSQL("create index Person.name on Person (name) unique")).execute();
      System.out.println("recreateIndexNode2: Node2 created index: " + result);
      Assert.assertEquals(expected, ((Number) result).intValue());
    } finally {
      database.close();
    }

    // CHECK ON NODE 1
    server = serverInstance.get(0);
    database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
    try {
      final long indexSize = database.getMetadata().getIndexManager().getIndex("Person.name").getSize();
      Assert.assertEquals(expected, indexSize);
      System.out.println("recreateIndexNode2: Node1 has the index too, ok");
    } finally {
      database.close();
    }
  }

  protected void checkIndexedEntries() {
    ODatabaseDocumentTx database;
    for (ServerRun server : serverInstance) {
      if (server.isActive()) {
        database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
        try {
          final long indexSize = database.getMetadata().getIndexManager().getIndex("Person.name").getSize();

          if (indexSize != expected) {
            // ERROR: DUMP ALL THE RECORDS
            List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select from index:Person.name"));
            int i = 0;
            for (ODocument d : result) {
              System.out.println((i++) + ": " + ((OIdentifiable) d.field("rid")).getRecord());
            }
          }

          Assert.assertEquals(expected, indexSize);

          System.out.println("From metadata: indexes " + indexSize + " items");

          List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from index:Person.name"));
          Assert.assertEquals(expected, ((Long) result.get(0).field("count")).longValue());

          System.out.println("From sql: indexes " + indexSize + " items");
        } finally {
          database.close();
        }
      }
    }
  }

  protected void checkInsertedEntries() {
    ODatabaseDocumentTx database;
    for (ServerRun server : serverInstance) {
      if (server.isActive()) {
        database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
        try {
          final int total = (int) database.countClass("Person");

          // if (total != expected) {
          // // ERROR: DUMP ALL THE RECORDS
          // result = database.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
          // i = 0;
          // for (ODocument d : result) {
          // System.out.println((i++) + ": " + d);
          // }
          // }

          Assert.assertEquals(expected, total);
        } finally {
          database.close();
        }
      }
    }
  }

  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new Writer(serverId, threadId, databaseURL);
  }

  private void printStats(final String databaseUrl) {
    final ODatabaseDocumentTx database = poolFactory.get(databaseUrl, "admin", "admin").acquire();
    try {
      database.reload();
      List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));

      final String name = database.getURL();

      System.out.println("\nReader " + name + " sql count: " + result.get(0) + " counting class: " + database.countClass("Person")
          + " counting cluster: " + database.countClusterElements("Person"));

      if (((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().existsClass("ODistributedConflict"))
        try {
          List<ODocument> conflicts = database
              .query(new OSQLSynchQuery<OIdentifiable>("select count(*) from ODistributedConflict"));
          long totalConflicts = conflicts.get(0).field("count");
          Assert.assertEquals(0l, totalConflicts);
          System.out.println("\nReader " + name + " conflicts: " + totalConflicts);
        } catch (OQueryParsingException e) {
          // IGNORE IT
        }

    } finally {
      database.close();
    }

  }
}
