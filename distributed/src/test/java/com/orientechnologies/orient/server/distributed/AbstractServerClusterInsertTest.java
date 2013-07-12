/*
 * Copyright 2010-2013 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.distributed;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Insert records concurrently against the cluster
 */
public abstract class AbstractServerClusterInsertTest extends AbstractServerClusterTest {
  protected static final int delayWriter = 0;
  protected static final int delayReader = 1000;
  protected int              count       = 1000;
  protected long             beginInstances;

  public String getDatabaseName() {
    return "distributed";
  }

  protected abstract String getDatabaseURL(ServerRun server);

  /**
   * Event called right after the database has been created and right before to be replicated to the X servers
   * 
   * @param db
   *          Current database
   */
  protected void onAfterDatabaseCreation(final ODatabaseDocumentTx db) {
    System.out.println("Creating database schema...");

    // CREATE BASIC SCHEMA
    OClass personClass = db.getMetadata().getSchema().createClass("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("name", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.INTEGER);

    final OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.getClass("Person");
    person.createIndex("Person.name", INDEX_TYPE.UNIQUE, "name");

    OClass customer = schema.createClass("Customer", person);
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = schema.createClass("Provider", person);
    provider.createProperty("totalPurchased", OType.DECIMAL);

    new ODocument("Customer").fields("name", "Jay", "surname", "Miner").save();
    new ODocument("Customer").fields("name", "Luke", "surname", "Skywalker").save();
    new ODocument("Provider").fields("name", "Yoda", "surname", "Nothing").save();
  }

  private void dropIndexNode1() {
    ServerRun server = serverInstance.get(0);
    ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      Object result = database.command(new OCommandSQL("drop index Person.name")).execute();
      System.out.println("dropIndexNode1: Node1 drop index: " + result);
    } finally {
      database.close();
    }

    // CHECK ON NODE 1
    server = serverInstance.get(1);
    database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      database.getMetadata().getIndexManager().reload();
      Assert.assertNull(database.getMetadata().getIndexManager().getIndex("Person.name"));
      System.out.println("dropIndexNode1: Node2 hasn't the index too, ok");
    } finally {
      database.close();
    }
  }

  private void recreateIndexNode2() {
    // RE-CREATE INDEX ON NODE 1
    ServerRun server = serverInstance.get(1);
    ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      Object result = database.command(new OCommandSQL("create index Person.name on Person (name) unique")).execute();
      System.out.println("recreateIndexNode2: Node2 created index: " + result);
      Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances, result);
    } finally {
      database.close();
    }

    // CHECK ON NODE 1
    server = serverInstance.get(0);
    database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
    try {
      final long indexSize = database.getMetadata().getIndexManager().getIndex("Person.name").getSize();
      Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances, indexSize);
      System.out.println("recreateIndexNode2: Node1 has the index too, ok");
    } finally {
      database.close();
    }
  }

  public void executeTest() throws Exception {

    ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(serverInstance.get(0)), "admin", "admin");
    try {
      List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
      beginInstances = ((Long) result.get(0).field("count")).longValue();
    } finally {
      database.close();
    }

    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService writerExecutor = Executors.newCachedThreadPool();
    final ExecutorService readerExecutor = Executors.newCachedThreadPool();

    int i = 0;
    for (ServerRun server : serverInstance) {
      Writer writer = new Writer(i++, getDatabaseURL(server));
      writerExecutor.submit(writer);

      Reader reader = new Reader(getDatabaseURL(server));
      readerExecutor.submit(reader);
    }

    System.out.println("Threads started, waiting for the end");

    writerExecutor.shutdown();
    Assert.assertTrue(writerExecutor.awaitTermination(300, TimeUnit.MINUTES));

    System.out.println("Writer threads finished, shutting down Reader threads...");

    readerExecutor.shutdownNow();
    Assert.assertTrue(readerExecutor.awaitTermination(10, TimeUnit.SECONDS));

    System.out.println("All threads have finished, shutting down server instances");

    for (ServerRun server : serverInstance) {
      printStats(getDatabaseURL(server));
    }

    checkInsertedEntries();
    checkIndexedEntries();
    dropIndexNode1();
    recreateIndexNode2();
  }

  private void checkIndexedEntries() {
    ODatabaseDocumentTx database;
    for (ServerRun server : serverInstance) {
      database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
      try {
        final long indexSize = database.getMetadata().getIndexManager().getIndex("Person.name").getSize();
        Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances, indexSize);

        System.out.println("From metadata: indexes " + indexSize + " items");

        List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from index:Person.name"));
        Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances,
            ((Long) result.get(0).field("count")).longValue());

        System.out.println("From sql: indexes " + indexSize + " items");
      } finally {
        database.close();
      }
    }
  }

  private void checkInsertedEntries() {
    ODatabaseDocumentTx database;
    int i;
    for (ServerRun server : serverInstance) {
      database = ODatabaseDocumentPool.global().acquire(getDatabaseURL(server), "admin", "admin");
      try {
        List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
        final long total = ((Long) result.get(0).field("count")).longValue();

        if (total != (long) (count * serverInstance.size()) + beginInstances) {
          // ERROR: DUMP ALL THE RECORDS
          result = database.query(new OSQLSynchQuery<OIdentifiable>("select from Person"));
          i = 0;
          for (ODocument d : result) {
            System.out.println((i++) + ": " + d);
          }
        }

        Assert.assertEquals((long) (count * serverInstance.size()) + beginInstances, total);
      } finally {
        database.close();
      }
    }
  }

  class Writer implements Runnable {
    private final String databaseUrl;
    private int          serverId;

    public Writer(final int iServerId, final String db) {
      serverId = iServerId;
      databaseUrl = db;
    }

    @Override
    public void run() {
      String name = null;
      for (int i = 0; i < count; i++) {
        final ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
        try {
          if (name == null)
            name = database.getURL();

          if ((i + 1) % 1 == 0)
            System.out.println("\nWriter " + name + " created " + (i + 1) + "/" + count + " records so far");

          final int uniqueId = count * serverId + i;

          ODocument person = new ODocument("Person").fields("id", UUID.randomUUID().toString(), "name", "Billy" + uniqueId,
              "surname", "Mayes" + uniqueId, "birthday", new Date(), "children", uniqueId);
          database.save(person);

          Thread.sleep(delayWriter);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + database.getURL());
          Thread.currentThread().interrupt();
          break;
        } finally {
          database.close();
        }
      }

      System.out.println("\nWriter " + name + " END");
    }
  }

  class Reader implements Runnable {
    private final String databaseUrl;

    public Reader(final String db) {
      databaseUrl = db;
    }

    @Override
    public void run() {
      try {
        while (!Thread.interrupted()) {
          try {
            printStats(databaseUrl);
            Thread.sleep(delayReader);

          } catch (Exception e) {
            break;
          }
        }

      } finally {
        printStats(databaseUrl);
      }
    }
  }

  private void printStats(final String databaseUrl) {
    final ODatabaseDocumentTx database = ODatabaseDocumentPool.global().acquire(databaseUrl, "admin", "admin");
    try {
      List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));

      final String name = database.getURL();

      System.out.println("\nReader " + name + " sql count: " + result.get(0) + " counting class: " + database.countClass("Person")
          + " counting cluster: " + database.countClusterElements("Person"));

      if (database.getMetadata().getSchema().existsClass("ODistributedConflict"))
        try {
          List<ODocument> conflicts = database
              .query(new OSQLSynchQuery<OIdentifiable>("select count(*) from ODistributedConflict"));
          long totalConflicts = (Long) conflicts.get(0).field("count");
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
